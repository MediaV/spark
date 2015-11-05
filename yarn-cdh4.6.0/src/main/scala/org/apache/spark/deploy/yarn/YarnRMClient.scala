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

import java.net.URI
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.{Map, Set}
import scala.util.Try

import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.{AMRMProtocol, ApplicationConstants}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.util.{Records, ConverterUtils}
import org.apache.hadoop.yarn.ipc.YarnRPC

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.util.Utils

/**
 * Handles registering and unregistering the application with the YARN ResourceManager.
 */
private[spark] class YarnRMClient(args: ApplicationMasterArguments) extends Logging {

  private var rpc: YarnRPC = null
  private var amClient: AMRMProtocol = _
  private var uiHistoryAddress: String = _
  private var registered: Boolean = false

  /**
   * Registers the application master with the RM.
   *
   * @param conf The Yarn configuration.
   * @param sparkConf The Spark configuration.
   * @param preferredNodeLocations Map with hints about where to allocate containers.
   * @param uiAddress Address of the SparkUI.
   * @param uiHistoryAddress Address of the application on the History Server.
   */
  def register(
      driverUrl: String,
      driverRef: RpcEndpointRef,
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      preferredNodeLocations: Map[String, Set[SplitInfo]],
      uiAddress: String,
      uiHistoryAddress: String,
      securityMgr: SecurityManager
    ): YarnAllocator = {
    this.rpc = YarnRPC.create(conf)
    this.uiHistoryAddress = uiHistoryAddress

    logInfo("Registering the ApplicationMaster")
    synchronized {
      amClient = registerWithResourceManager(conf)
      registerApplicationMaster(uiAddress)
      registered = true
    }
    new YarnAllocator(driverUrl, driverRef, conf, sparkConf, amClient, getAttemptId(), 
        args, preferredNodeLocations, securityMgr)
  }

  /**
   * Unregister the AM. Guaranteed to only be called once.
   *
   * @param status The final status of the AM.
   * @param diagnostics Diagnostics message to include in the final status.
   */
  def unregister(status: FinalApplicationStatus, diagnostics: String = ""): Unit = synchronized {
    if (registered) {
      val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest])
        .asInstanceOf[FinishApplicationMasterRequest]
      finishReq.setAppAttemptId(getAttemptId())
      finishReq.setFinishApplicationStatus(status)
      finishReq.setDiagnostics(diagnostics)
      finishReq.setTrackingUrl(uiHistoryAddress)
      amClient.finishApplicationMaster(finishReq)
    }
  }

  /** Returns the attempt ID. */
  def getAttemptId(): ApplicationAttemptId = {
    YarnSparkHadoopUtil.get.getContainerId.getApplicationAttemptId()
  }

  /** Returns the configuration for the AmIpFilter to add to the Spark UI. */
  def getAmIpFilterParams(conf: YarnConfiguration, proxyBase: String): Map[String, String] = {
    val proxy = YarnConfiguration.getProxyHostAndPort(conf)
    val parts = proxy.split(":")
    val uriBase = "http://" + proxy + proxyBase
    Map("PROXY_HOST" -> parts(0), "PROXY_URI_BASE" -> uriBase)
  }

  /** Returns the maximum number of attempts to register the AM. */
  def getMaxRegAttempts(sparkConf: SparkConf, yarnConf: YarnConfiguration): Int = {
    val sparkMaxAttempts = sparkConf.getOption("spark.yarn.maxAppAttempts").map(_.toInt)
    val yarnMaxAttempts = yarnConf.getInt(
      YarnConfiguration.RM_AM_MAX_RETRIES, YarnConfiguration.DEFAULT_RM_AM_MAX_RETRIES)
    val retval: Int = sparkMaxAttempts match {
      case Some(x) => if (x <= yarnMaxAttempts) x else yarnMaxAttempts
      case None => yarnMaxAttempts
    }

    retval
  }

  private def registerWithResourceManager(conf: YarnConfiguration): AMRMProtocol = {
    val rmAddress = NetUtils.createSocketAddr(conf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    logInfo("Connecting to ResourceManager at " + rmAddress)
    rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]
  }

  private def registerApplicationMaster(uiAddress: String): RegisterApplicationMasterResponse = {
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
      .asInstanceOf[RegisterApplicationMasterRequest]
    appMasterRequest.setApplicationAttemptId(getAttemptId())
    // Setting this to master host,port - so that the ApplicationReport at client has some
    // sensible info.
    // Users can then monitor stderr/stdout on that node if required.
    appMasterRequest.setHost(Utils.localHostName())
    appMasterRequest.setRpcPort(0)
    // remove the scheme from the url if it exists since Hadoop does not expect scheme
    val uri = new URI(uiAddress)
    val authority = if (uri.getScheme == null) uiAddress else uri.getAuthority
    appMasterRequest.setTrackingUrl(authority)
    amClient.registerApplicationMaster(appMasterRequest)
  }

}
