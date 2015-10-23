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

import java.lang.{Boolean => JBoolean}
import java.io.File
import java.util.{Collections, Set => JSet}
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.mutable.HashMap
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{Master, JobConf}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ApplicationAccessType, ContainerId, Priority}
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.RackResolver

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.util.Utils

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  private var tokenRenewer: Option[ExecutorDelegationTokenUpdater] = None

  override def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    dest.addCredentials(source.getCredentials())
  }

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn
  // mode, this MUST be set to true.
  override def isYarnMode(): Boolean = { true }

  // Return an appropriate (subclass) of Configuration. Creating a config initializes some Hadoop
  // subsystems. Always create a new config, dont reuse yarnConf.
  override def newConfiguration(conf: SparkConf): Configuration =
    new YarnConfiguration(super.newConfiguration(conf))

  // Add any user credentials to the job conf which are necessary for running on a secure Hadoop
  // cluster
  override def addCredentials(conf: JobConf) {
    val jobCreds = conf.getCredentials()
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
  }

  override def getCurrentUserCredentials(): Credentials = {
    UserGroupInformation.getCurrentUser().getCredentials()
  }

  override def addCurrentUserCredentials(creds: Credentials) {
    UserGroupInformation.getCurrentUser().addCredentials(creds)
  }

  override def addSecretKeyToUserCredentials(key: String, secret: String) {
    val creds = new Credentials()
    creds.addSecretKey(new Text(key), secret.getBytes("utf-8"))
    addCurrentUserCredentials(creds)
  }

  override def getSecretKeyFromUserCredentials(key: String): Array[Byte] = {
    val credentials = getCurrentUserCredentials()
    if (credentials != null) credentials.getSecretKey(new Text(key)) else null
  }

  /**
   * Get the list of namenodes the user may access.
   */
  def getNameNodesToAccess(sparkConf: SparkConf): Set[Path] = {
    sparkConf.get("spark.yarn.access.namenodes", "")
      .split(",")
      .map(_.trim())
      .filter(!_.isEmpty)
      .map(new Path(_))
      .toSet
  }

  def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }
    delegTokenRenewer
  }

  /**
   * Obtains tokens for the namenodes passed in and adds them to the credentials.
   */
  def obtainTokensForNamenodes(
    paths: Set[Path],
    conf: Configuration,
    creds: Credentials,
    renewer: Option[String] = None
  ): Unit = {
    if (UserGroupInformation.isSecurityEnabled()) {
      val delegTokenRenewer = renewer.getOrElse(getTokenRenewer(conf))
      paths.foreach { dst =>
        val dstFs = dst.getFileSystem(conf)
        logInfo("getting token for namenode: " + dst)
        dstFs.addDelegationTokens(delegTokenRenewer, creds)
      }
    }
  }

  private[spark] override def startExecutorDelegationTokenRenewer(sparkConf: SparkConf): Unit = {
    tokenRenewer = Some(new ExecutorDelegationTokenUpdater(sparkConf, conf))
    tokenRenewer.get.updateCredentialsIfRequired()
  }

  private[spark] override def stopExecutorDelegationTokenRenewer(): Unit = {
    tokenRenewer.foreach(_.stop())
  }

  private[spark] def getContainerId: ContainerId = {
    val containerIdString = System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV)
    ConverterUtils.toContainerId(containerIdString)
  }
}

object YarnSparkHadoopUtil {
  // Additional memory overhead
  // 10% was arrived at experimentally. In the interest of minimizing memory waste while covering
  // the common cases. Memory overhead tends to grow with container size.

  val MEMORY_OVERHEAD_FACTOR = 0.10
  val MEMORY_OVERHEAD_MIN = 384

  val ANY_HOST = "*"

  val DEFAULT_NUMBER_EXECUTORS = 2

  // All RM requests are issued with same priority : we do not (yet) have any distinction between
  // request types (like map/reduce in hadoop for example)
  val RM_REQUEST_PRIORITY = 1

  // Host to rack map - saved from allocation requests. We are expecting this not to change.
  // Note that it is possible for this to change : and ResourceManager will indicate that to us via
  // update response to allocate. But we are punting on handling that for now.
  private val hostToRack = new ConcurrentHashMap[String, String]()
  private val rackToHostSet = new ConcurrentHashMap[String, JSet[String]]()

  def get: YarnSparkHadoopUtil = {
    val yarnMode = java.lang.Boolean.valueOf(
      System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if (!yarnMode) {
      throw new SparkException("YarnSparkHadoopUtil is not available in non-YARN mode!")
    }
    SparkHadoopUtil.get.asInstanceOf[YarnSparkHadoopUtil]
  }
  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
   */
  def addPathToEnvironment(env: HashMap[String, String], key: String, value: String): Unit = {
    val newValue = if (env.contains(key)) { env(key) + getClassPathSeparator  + value } else value
    env.put(key, newValue)
  }

  /**
   * Set zero or more environment variables specified by the given input string.
   * The input string is expected to take the form "KEY1=VAL1,KEY2=VAL2,KEY3=VAL3".
   */
  def setEnvFromInputString(env: HashMap[String, String], inputString: String): Unit = {
    if (inputString != null && inputString.length() > 0) {
      val childEnvs = inputString.split(",")
      val p = Pattern.compile(environmentVariableRegex)
      for (cEnv <- childEnvs) {
        val parts = cEnv.split("=") // split on '='
        val m = p.matcher(parts(1))
        val sb = new StringBuffer
        while (m.find()) {
          val variable = m.group(1)
          var replace = ""
          if (env.get(variable) != None) {
            replace = env.get(variable).get
          } else {
            // if this key is not configured for the child .. get it from the env
            replace = System.getenv(variable)
            if (replace == null) {
            // the env key is note present anywhere .. simply set it
              replace = ""
            }
          }
          m.appendReplacement(sb, Matcher.quoteReplacement(replace))
        }
        m.appendTail(sb)
        // This treats the environment variable as path variable delimited by `File.pathSeparator`
        // This is kept for backward compatibility and consistency with Hadoop's behavior
        addPathToEnvironment(env, parts(0), sb.toString)
      }
    }
  }

  private val environmentVariableRegex: String = {
    if (Utils.isWindows) {
      "%([A-Za-z_][A-Za-z0-9_]*?)%"
    } else {
      "\\$([A-Za-z_][A-Za-z0-9_]*)"
    }
  }

  /**
   * Escapes a string for inclusion in a command line executed by Yarn. Yarn executes commands
   * using `bash -c "command arg1 arg2"` and that means plain quoting doesn't really work. The
   * argument is enclosed in single quotes and some key characters are escaped.
   *
   * @param arg A single argument.
   * @return Argument quoted for execution via Yarn's generated shell script.
   */
  def escapeForShell(arg: String): String = {
    if (arg != null) {
      val escaped = new StringBuilder("'")
      for (i <- 0 to arg.length() - 1) {
        arg.charAt(i) match {
          case '$' => escaped.append("\\$")
          case '"' => escaped.append("\\\"")
          case '\'' => escaped.append("'\\''")
          case c => escaped.append(c)
        }
      }
      escaped.append("'").toString()
    } else {
      arg
    }
  }

  def getApplicationAclsForYarn(securityMgr: SecurityManager)
      : Map[ApplicationAccessType, String] = {
    Map[ApplicationAccessType, String] (
      ApplicationAccessType.VIEW_APP -> securityMgr.getViewAcls,
      ApplicationAccessType.MODIFY_APP -> securityMgr.getModifyAcls
    )
  }

  /**
   * Expand environment variable using Yarn API.
   * If environment.$$() is implemented, return the result of it.
   * Otherwise, return the result of environment.$()
   * Note: $$() is added in Hadoop 2.4.
   */
  private lazy val expandMethod =
    Try(classOf[Environment].getMethod("$$"))
      .getOrElse(classOf[Environment].getMethod("$"))

  def expandEnvironment(environment: Environment): String =
    expandMethod.invoke(environment).asInstanceOf[String]

  /**
   * Get class path separator using Yarn API.
   * If ApplicationConstants.CLASS_PATH_SEPARATOR is implemented, return it.
   * Otherwise, return File.pathSeparator
   * Note: CLASS_PATH_SEPARATOR is added in Hadoop 2.4.
   */
  private lazy val classPathSeparatorField =
    Try(classOf[ApplicationConstants].getField("CLASS_PATH_SEPARATOR"))
      .getOrElse(classOf[File].getField("pathSeparator"))

  def getClassPathSeparator(): String = {
    classPathSeparatorField.get(null).asInstanceOf[String]
  }

  def lookupRack(conf: Configuration, host: String): String = {
    if (!hostToRack.contains(host)) {
      populateRackInfo(conf, host)
    }
    hostToRack.get(host)
  }

  def populateRackInfo(conf: Configuration, hostname: String) {
    Utils.checkHost(hostname)

    if (!hostToRack.containsKey(hostname)) {
      // If there are repeated failures to resolve, all to an ignore list.
      val rackInfo = RackResolver.resolve(conf, hostname)
      if (rackInfo != null && rackInfo.getNetworkLocation != null) {
        val rack = rackInfo.getNetworkLocation
        hostToRack.put(hostname, rack)
        if (! rackToHostSet.containsKey(rack)) {
          rackToHostSet.putIfAbsent(rack,
            Collections.newSetFromMap(new ConcurrentHashMap[String, JBoolean]()))
        }
        rackToHostSet.get(rack).add(hostname)

        // TODO(harvey): Figure out what this comment means...
        // Since RackResolver caches, we are disabling this for now ...
      } /* else {
        // right ? Else we will keep calling rack resolver in case we cant resolve rack info ...
        hostToRack.put(hostname, null)
      } */
    }
  }
}

