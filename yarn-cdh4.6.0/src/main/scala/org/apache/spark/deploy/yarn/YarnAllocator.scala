/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import org.apache.hadoop.yarn.api.AMRMProtocol
import org.apache.hadoop.yarn.util.Records

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords.{AllocateRequest, AllocateResponse}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.scheduler.{SplitInfo, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._

object AllocationType extends Enumeration {
  type AllocationType = Value
  val HOST, RACK, ANY = Value
}

// TODO:
// Too many params.
// Needs to be mt-safe
// Need to refactor this to make it 'cleaner' ... right now, all computation is reactive - should
// make it more proactive and decoupled.

// Note that right now, we assume all node asks as uniform in terms of capabilities and priority
// Refer to http://developer.yahoo.com/blogs/hadoop/posts/2011/03/mapreduce-nextgen-scheduler/ for
// more info on how we are requesting for containers.

/**
 * Common code for the Yarn container allocator. Contains all the version-agnostic code to
 * manage container allocation for a running Spark application.
 */
private[yarn] class YarnAllocator(
    conf: Configuration,
    sparkConf: SparkConf,
    amClient: AMRMProtocol,
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    preferredNodes: collection.Map[String, collection.Set[SplitInfo]],
    securityMgr: SecurityManager)
  extends Logging {

  import YarnAllocator._

  // These three are locked on allocatedHostToContainersMap. Complementary data structures
  // allocatedHostToContainersMap : containers which are running : host, Set<containerid>
  // allocatedContainerToHostMap: container to host mapping.
  private val allocatedHostToContainersMap =
    new HashMap[String, collection.mutable.Set[ContainerId]]()

  private val allocatedContainerToHostMap = new HashMap[ContainerId, String]()

  // allocatedRackCount is populated ONLY if allocation happens (or decremented if this is an
  // allocated node)
  // As with the two data structures above, tightly coupled with them, and to be locked on
  // allocatedHostToContainersMap
  private val allocatedRackCount = new HashMap[String, Int]()

  // Containers to be released in next request to RM
  private val releasedContainers = new ConcurrentHashMap[ContainerId, Boolean]

  private val lastResponseId = new AtomicInteger()
  private val releaseList: CopyOnWriteArrayList[ContainerId] = new CopyOnWriteArrayList()

  // Number of container requests that have been sent to, but not yet allocated by the
  // ApplicationMaster.
  private val numPendingAllocate = new AtomicInteger()
  private val numExecutorsRunning = new AtomicInteger()
  // Used to generate a unique id per executor
  private val executorIdCounter = new AtomicInteger()
  private val numExecutorsFailed = new AtomicInteger()

  private var maxExecutors = args.numExecutors

  // Keep track of which container is running which executor to remove the executors later
  private val executorIdToContainer = new HashMap[String, Container]

  private val executorMemory = args.executorMemory
  private val executorCores = args.executorCores
  private val (preferredHostToCount, preferredRackToCount) =
    generateNodeToWeight(conf, preferredNodes)

  // Additional memory overhead - in mb.
  private val memoryOverhead: Int = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))

  private val launcherPool = new ThreadPoolExecutor(
    // max pool size of Integer.MAX_VALUE is ignored because we use an unbounded queue
    sparkConf.getInt("spark.yarn.containerLauncherMaxThreads", 25), Integer.MAX_VALUE,
    1, TimeUnit.MINUTES,
    new LinkedBlockingQueue[Runnable](),
    new ThreadFactoryBuilder().setNameFormat("ContainerLauncher #%d").setDaemon(true).build())
  launcherPool.allowCoreThreadTimeOut(true)

  def getNumExecutorsRunning: Int = numExecutorsRunning.intValue

  def getNumExecutorsFailed: Int = numExecutorsFailed.intValue

  /**
   * Request as many executors from the ResourceManager as needed to reach the desired total.
   * This takes into account executors already running or pending.
   */
  def requestTotalExecutors(requestedTotal: Int): Unit = synchronized {
    val currentTotal = numPendingAllocate.get + numExecutorsRunning.get
    if (requestedTotal > currentTotal) {
      maxExecutors += (requestedTotal - currentTotal)
      // We need to call `allocateResources` here to avoid the following race condition:
      // If we request executors twice before `allocateResources` is called, then we will end up
      // double counting the number requested because `numPendingAllocate` is not updated yet.
      allocateResources()
    } else {
      logInfo(s"Not allocating more executors because there are already $currentTotal " +
        s"(application requested $requestedTotal total)")
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   */
  def killExecutor(executorId: String): Unit = synchronized {
    if (executorIdToContainer.contains(executorId)) {
      val container = executorIdToContainer.remove(executorId).get
      internalReleaseContainer(container)
      numExecutorsRunning.decrementAndGet()
      maxExecutors -= 1
      assert(maxExecutors >= 0, "Allocator killed more executors than are allocated!")
    } else {
      logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  /**
   * Allocate missing containers based on the number of executors currently pending and running.
   *
   * This method prioritizes the allocated container responses from the RM based on node and
   * rack locality. Additionally, it releases any extra containers allocated for this application
   * but are not needed. This must be synchronized because variables read in this block are
   * mutated by other methods.
   */
  def allocateResources(): Unit = synchronized {
    val missing = maxExecutors - numPendingAllocate.get() - numExecutorsRunning.get()

    // this is needed by alpha, do it here since we add numPending right after this
    val executorsPending = numPendingAllocate.get()
    if (missing > 0) {
      val totalExecutorMemory = executorMemory + memoryOverhead
      numPendingAllocate.addAndGet(missing)
      logInfo(s"Will allocate $missing executor containers, each with $totalExecutorMemory MB " +
        s"memory including $memoryOverhead MB overhead")
    } else {
      logDebug("Empty allocation request ...")
    }

    val allocateResponse = allocateContainers(missing, executorsPending)
    val allocatedContainers = allocateResponse.getAllocatedContainers()

    if (allocatedContainers.size > 0) {
      var numPendingAllocateNow = numPendingAllocate.addAndGet(-1 * allocatedContainers.size)

      if (numPendingAllocateNow < 0) {
        numPendingAllocateNow = numPendingAllocate.addAndGet(-1 * numPendingAllocateNow)
      }

      logDebug("""
        Allocated containers: %d
        Current executor count: %d
        Containers released: %s
        Cluster resources: %s
        """.format(
          allocatedContainers.size,
          numExecutorsRunning.get(),
          releasedContainers,
          allocateResponse.getAvailableResources))

      val hostToContainers = new HashMap[String, ArrayBuffer[Container]]()

      for (container <- allocatedContainers) {
        if (isResourceConstraintSatisfied(container)) {
          // Add the accepted `container` to the host's list of already accepted,
          // allocated containers
          val host = container.getNodeId.getHost
          val containersForHost = hostToContainers.getOrElseUpdate(host,
            new ArrayBuffer[Container]())
          containersForHost += container
        } else {
          // Release container, since it doesn't satisfy resource constraints.
          internalReleaseContainer(container)
        }
      }

      // Find the appropriate containers to use.
      // TODO: Cleanup this group-by...
      val dataLocalContainers = new HashMap[String, ArrayBuffer[Container]]()
      val rackLocalContainers = new HashMap[String, ArrayBuffer[Container]]()
      val offRackContainers = new HashMap[String, ArrayBuffer[Container]]()

      for (candidateHost <- hostToContainers.keySet) {
        val maxExpectedHostCount = preferredHostToCount.getOrElse(candidateHost, 0)
        val requiredHostCount = maxExpectedHostCount - allocatedContainersOnHost(candidateHost)

        val remainingContainersOpt = hostToContainers.get(candidateHost)
        assert(remainingContainersOpt.isDefined)
        var remainingContainers = remainingContainersOpt.get

        if (requiredHostCount >= remainingContainers.size) {
          // Since we have <= required containers, add all remaining containers to
          // `dataLocalContainers`.
          dataLocalContainers.put(candidateHost, remainingContainers)
          // There are no more free containers remaining.
          remainingContainers = null
        } else if (requiredHostCount > 0) {
          // Container list has more containers than we need for data locality.
          // Split the list into two: one based on the data local container count,
          // (`remainingContainers.size` - `requiredHostCount`), and the other to hold remaining
          // containers.
          val (dataLocal, remaining) = remainingContainers.splitAt(
            remainingContainers.size - requiredHostCount)
          dataLocalContainers.put(candidateHost, dataLocal)

          // Invariant: remainingContainers == remaining

          // YARN has a nasty habit of allocating a ton of containers on a host - discourage this.
          // Add each container in `remaining` to list of containers to release. If we have an
          // insufficient number of containers, then the next allocation cycle will reallocate
          // (but won't treat it as data local).
          // TODO(harvey): Rephrase this comment some more.
          for (container <- remaining) internalReleaseContainer(container)
          remainingContainers = null
        }

        // For rack local containers
        if (remainingContainers != null) {
          val rack = YarnSparkHadoopUtil.lookupRack(conf, candidateHost)
          if (rack != null) {
            val maxExpectedRackCount = preferredRackToCount.getOrElse(rack, 0)
            val requiredRackCount = maxExpectedRackCount - allocatedContainersOnRack(rack) -
              rackLocalContainers.getOrElse(rack, List()).size

            if (requiredRackCount >= remainingContainers.size) {
              // Add all remaining containers to to `dataLocalContainers`.
              dataLocalContainers.put(rack, remainingContainers)
              remainingContainers = null
            } else if (requiredRackCount > 0) {
              // Container list has more containers that we need for data locality.
              // Split the list into two: one based on the data local container count,
              // (`remainingContainers.size` - `requiredHostCount`), and the other to hold
              // remaining containers.
              val (rackLocal, remaining) = remainingContainers.splitAt(
                remainingContainers.size - requiredRackCount)
              val existingRackLocal = rackLocalContainers.getOrElseUpdate(rack,
                new ArrayBuffer[Container]())

              existingRackLocal ++= rackLocal

              remainingContainers = remaining
            }
          }
        }

        if (remainingContainers != null) {
          // Not all containers have been consumed - add them to the list of off-rack containers.
          offRackContainers.put(candidateHost, remainingContainers)
        }
      }

      // Now that we have split the containers into various groups, go through them in order:
      // first host-local, then rack-local, and finally off-rack.
      // Note that the list we create below tries to ensure that not all containers end up within
      // a host if there is a sufficiently large number of hosts/containers.
      val allocatedContainersToProcess = new ArrayBuffer[Container](allocatedContainers.size)
      allocatedContainersToProcess ++= TaskSchedulerImpl.prioritizeContainers(dataLocalContainers)
      allocatedContainersToProcess ++= TaskSchedulerImpl.prioritizeContainers(rackLocalContainers)
      allocatedContainersToProcess ++= TaskSchedulerImpl.prioritizeContainers(offRackContainers)

      // Run each of the allocated containers.
      for (container <- allocatedContainersToProcess) {
        val numExecutorsRunningNow = numExecutorsRunning.incrementAndGet()
        val executorHostname = container.getNodeId.getHost
        val containerId = container.getId

        val executorMemoryOverhead = (executorMemory + memoryOverhead)
        assert(container.getResource.getMemory >= executorMemoryOverhead)

        if (numExecutorsRunningNow > maxExecutors) {
          logInfo("""Ignoring container %s at host %s, since we already have the required number of
            containers for it.""".format(containerId, executorHostname))
          internalReleaseContainer(container)
          numExecutorsRunning.decrementAndGet()
        } else {
          val executorId = executorIdCounter.incrementAndGet().toString
          val driverUrl = "akka.tcp://%s@%s:%s/user/%s".format(
            SparkEnv.driverActorSystemName,
            sparkConf.get("spark.driver.host"),
            sparkConf.get("spark.driver.port"),
            CoarseGrainedSchedulerBackend.ENDPOINT_NAME)

          logInfo("Launching container %s for on host %s".format(containerId, executorHostname))
          executorIdToContainer(executorId) = container

          // To be safe, remove the container from `releasedContainers`.
          releasedContainers.remove(containerId)

          val rack = YarnSparkHadoopUtil.lookupRack(conf, executorHostname)
          allocatedHostToContainersMap.synchronized {
            val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
              new HashSet[ContainerId]())

            containerSet += containerId
            allocatedContainerToHostMap.put(containerId, executorHostname)

            if (rack != null) {
              allocatedRackCount.put(rack, allocatedRackCount.getOrElse(rack, 0) + 1)
            }
          }
          logInfo("Launching ExecutorRunnable. driverUrl: %s,  executorHostname: %s".format(
            driverUrl, executorHostname))
          val executorRunnable = new ExecutorRunnable(
            container,
            conf,
            sparkConf,
            driverUrl,
            executorId,
            executorHostname,
            executorMemory,
            executorCores,
            appAttemptId.getApplicationId.toString,
            securityMgr)
          launcherPool.execute(executorRunnable)
        }
      }
      logDebug("""
        Finished allocating %s containers (from %s originally).
        Current number of executors running: %d,
        Released containers: %s
        """.format(
          allocatedContainersToProcess,
          allocatedContainers,
          numExecutorsRunning.get(),
          releasedContainers))
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))

      for (completedContainer <- completedContainers) {
        val containerId = completedContainer.getContainerId

        if (releasedContainers.containsKey(containerId)) {
          // YarnAllocationHandler already marked the container for release, so remove it from
          // `releasedContainers`.
          releasedContainers.remove(containerId)
        } else {
          // Decrement the number of executors running. The next iteration of
          // the ApplicationMaster's reporting thread will take care of allocating.
          numExecutorsRunning.decrementAndGet()
          logInfo("Completed container %s (state: %s, exit status: %s)".format(
            containerId,
            completedContainer.getState,
            completedContainer.getExitStatus))
          // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
          // there are some exit status' we shouldn't necessarily count against us, but for
          // now I think its ok as none of the containers are expected to exit
          if (completedContainer.getExitStatus == -103) {
            // vmem limit exceeded
            logWarning(memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              VMEM_EXCEEDED_PATTERN))
          } else if (completedContainer.getExitStatus == -104) {
            // pmem limit exceeded
            logWarning(memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              PMEM_EXCEEDED_PATTERN))
          } else if (completedContainer.getExitStatus != 0) {
            logInfo("Container marked as failed: " + containerId +
              ". Exit status: " + completedContainer.getExitStatus +
              ". Diagnostics: " + completedContainer.getDiagnostics)
            numExecutorsFailed.incrementAndGet()
          }
        }

        allocatedHostToContainersMap.synchronized {
          if (allocatedContainerToHostMap.containsKey(containerId)) {
            val hostOpt = allocatedContainerToHostMap.get(containerId)
            assert(hostOpt.isDefined)
            val host = hostOpt.get

            val containerSetOpt = allocatedHostToContainersMap.get(host)
            assert(containerSetOpt.isDefined)
            val containerSet = containerSetOpt.get

            containerSet.remove(containerId)
            if (containerSet.isEmpty) {
              allocatedHostToContainersMap.remove(host)
            } else {
              allocatedHostToContainersMap.update(host, containerSet)
            }

            allocatedContainerToHostMap.remove(containerId)

            // TODO: Move this part outside the synchronized block?
            val rack = YarnSparkHadoopUtil.lookupRack(conf, host)
            if (rack != null) {
              val rackCount = allocatedRackCount.getOrElse(rack, 0) - 1
              if (rackCount > 0) {
                allocatedRackCount.put(rack, rackCount)
              } else {
                allocatedRackCount.remove(rack)
              }
            }
          }
        }
      }
      logDebug("""
        Finished processing %d completed containers.
        Current number of executors running: %d,
        Released containers: %s
        """.format(
          completedContainers.size,
          numExecutorsRunning.get(),
          releasedContainers))
    }
  }

  private def allocatedContainersOnHost(host: String): Int = {
    var retval = 0
    allocatedHostToContainersMap.synchronized {
      retval = allocatedHostToContainersMap.getOrElse(host, Set()).size
    }
    retval
  }

  private def allocatedContainersOnRack(rack: String): Int = {
    var retval = 0
    allocatedHostToContainersMap.synchronized {
      retval = allocatedRackCount.getOrElse(rack, 0)
    }
    retval
  }

  private def isResourceConstraintSatisfied(container: Container): Boolean = {
    container.getResource.getMemory >= (executorMemory + memoryOverhead)
  }

  // A simple method to copy the split info map.
  private def generateNodeToWeight(
      conf: Configuration,
      input: collection.Map[String, collection.Set[SplitInfo]]
    ): (Map[String, Int], Map[String, Int]) = {

    if (input == null) {
      return (Map[String, Int](), Map[String, Int]())
    }

    val hostToCount = new HashMap[String, Int]
    val rackToCount = new HashMap[String, Int]

    for ((host, splits) <- input) {
      val hostCount = hostToCount.getOrElse(host, 0)
      hostToCount.put(host, hostCount + splits.size)

      val rack = YarnSparkHadoopUtil.lookupRack(conf, host)
      if (rack != null) {
        val rackCount = rackToCount.getOrElse(host, 0)
        rackToCount.put(host, rackCount + splits.size)
      }
    }

    (hostToCount.toMap, rackToCount.toMap)
  }

  private def internalReleaseContainer(container: Container) = {
    releasedContainers.put(container.getId(), true)
    releaseContainer(container)
  }

  /**
   * Called to allocate containers in the cluster.
   *
   * @param count Number of containers to allocate.
   *              If zero, should still contact RM (as a heartbeat).
   * @param pending Number of containers pending allocate. Only used on alpha.
   * @return Response to the allocation request.
   */
  private def allocateContainers(count: Int, pending: Int): AllocateResponse = {
    var resourceRequests: List[ResourceRequest] = null

    logDebug("asking for additional executors: " + count + " with already pending: " + pending)
    val totalNumAsk = count + pending
    if (count <= 0) {
      resourceRequests = List()
    } else if (preferredHostToCount.isEmpty) {
      logDebug("host preferences is empty")
      resourceRequests = List(createResourceRequest(
        AllocationType.ANY, null, totalNumAsk, YarnSparkHadoopUtil.RM_REQUEST_PRIORITY))
    } else {
      // request for all hosts in preferred nodes and for numExecutors -
      // candidates.size, request by default allocation policy.
      val hostContainerRequests: ArrayBuffer[ResourceRequest] =
        new ArrayBuffer[ResourceRequest](preferredHostToCount.size)
      for ((candidateHost, candidateCount) <- preferredHostToCount) {
        val requiredCount = candidateCount - allocatedContainersOnHost(candidateHost)

        if (requiredCount > 0) {
          hostContainerRequests += createResourceRequest(
            AllocationType.HOST,
            candidateHost,
            requiredCount,
            YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)
        }
      }
      val rackContainerRequests: List[ResourceRequest] = createRackResourceRequests(
        hostContainerRequests.toList)

      val anyContainerRequests: ResourceRequest = createResourceRequest(
        AllocationType.ANY,
        resource = null,
        totalNumAsk,
        YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)

      val containerRequests: ArrayBuffer[ResourceRequest] = new ArrayBuffer[ResourceRequest](
        hostContainerRequests.size + rackContainerRequests.size + 1)

      containerRequests ++= hostContainerRequests
      containerRequests ++= rackContainerRequests
      containerRequests += anyContainerRequests

      resourceRequests = containerRequests.toList
    }

    val req = Records.newRecord(classOf[AllocateRequest])
    req.setResponseId(lastResponseId.incrementAndGet)
    req.setApplicationAttemptId(appAttemptId)

    req.addAllAsks(resourceRequests)

    val releasedContainerList = createReleasedContainerList()
    req.addAllReleases(releasedContainerList)

    if (count > 0) {
      logInfo("Allocating %d executor containers with %d of memory each.".format(totalNumAsk,
        executorMemory + memoryOverhead))
    } else {
      logDebug("Empty allocation req ..  release : " + releasedContainerList)
    }

    for (request <- resourceRequests) {
      logInfo("ResourceRequest (host : %s, num containers: %d, priority = %s , capability : %s)".
        format(
          request.getHostName,
          request.getNumContainers,
          request.getPriority,
          request.getCapability))
    }
    amClient.allocate(req)
  }

  private def createRackResourceRequests(hostContainers: List[ResourceRequest]):
  List[ResourceRequest] = {
    // First generate modified racks and new set of hosts under it : then issue requests
    val rackToCounts = new HashMap[String, Int]()

    // Within this lock - used to read/write to the rack related maps too.
    for (container <- hostContainers) {
      val candidateHost = container.getHostName
      val candidateNumContainers = container.getNumContainers
      assert(YarnSparkHadoopUtil.ANY_HOST != candidateHost)

      val rack = YarnSparkHadoopUtil.lookupRack(conf, candidateHost)
      if (rack != null) {
        var count = rackToCounts.getOrElse(rack, 0)
        count += candidateNumContainers
        rackToCounts.put(rack, count)
      }
    }

    val requestedContainers: ArrayBuffer[ResourceRequest] =
      new ArrayBuffer[ResourceRequest](rackToCounts.size)
    for ((rack, count) <- rackToCounts) {
      requestedContainers +=
        createResourceRequest(AllocationType.RACK, rack, count,
          YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)
    }

    requestedContainers.toList
  }


  private def createResourceRequest(
                                     requestType: AllocationType.AllocationType,
                                     resource: String,
                                     numExecutors: Int,
                                     priority: Int): ResourceRequest = {

    // If hostname specified, we need atleast two requests - node local and rack local.
    // There must be a third request - which is ANY : that will be specially handled.
    requestType match {
      case AllocationType.HOST => {
        assert(YarnSparkHadoopUtil.ANY_HOST != resource)
        val hostname = resource
        val nodeLocal = createResourceRequestImpl(hostname, numExecutors, priority)

        // Add to host->rack mapping
        YarnSparkHadoopUtil.populateRackInfo(conf, hostname)

        nodeLocal
      }
      case AllocationType.RACK => {
        val rack = resource
        createResourceRequestImpl(rack, numExecutors, priority)
      }
      case AllocationType.ANY => createResourceRequestImpl(
        YarnSparkHadoopUtil.ANY_HOST, numExecutors, priority)
      case _ => throw new IllegalArgumentException(
        "Unexpected/unsupported request type: " + requestType)
    }
  }

  private def createResourceRequestImpl(
                                         hostname: String,
                                         numExecutors: Int,
                                         priority: Int): ResourceRequest = {

    val rsrcRequest = Records.newRecord(classOf[ResourceRequest])
    val capability = Records.newRecord(classOf[Resource])
    // There probably is some overhead here, let's reserve a bit more memory.
    capability.setMemory(executorMemory + memoryOverhead)
    capability.setVirtualCores(executorCores)
    rsrcRequest.setCapability(capability)

    val pri = Records.newRecord(classOf[Priority])
    pri.setPriority(priority)
    rsrcRequest.setPriority(pri)

    rsrcRequest.setHostName(hostname)

    rsrcRequest.setNumContainers(java.lang.Math.max(numExecutors, 0))
    rsrcRequest
  }

  private def createReleasedContainerList(): ArrayBuffer[ContainerId] = {
    val retval = new ArrayBuffer[ContainerId](1)
    // Iterator on COW list ...
    for (container <- releaseList.iterator()) {
      retval += container
    }
    // Remove from the original list.
    if (!retval.isEmpty) {
      releaseList.removeAll(retval)
      logInfo("Releasing " + retval.size + " containers.")
    }
    retval
  }


  /** Called to release a previously allocated container. */
  private def releaseContainer(container: Container): Unit = {
    releaseList.add(container.getId())
  }

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val PMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX physical memory used")
  val VMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX virtual memory used")

  def memLimitExceededLogMessage(diagnostics: String, pattern: Pattern): String = {
    val matcher = pattern.matcher(diagnostics)
    val diag = if (matcher.find()) " " + matcher.group() + "." else ""
    ("Container killed by YARN for exceeding memory limits." + diag
      + " Consider boosting spark.yarn.executor.memoryOverhead.")
  }
}
