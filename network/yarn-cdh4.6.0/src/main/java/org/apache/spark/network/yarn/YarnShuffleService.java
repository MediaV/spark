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

package org.apache.spark.network.yarn;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.sasl.SaslRpcHandler;
import org.apache.spark.network.sasl.ShuffleSecretManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.yarn.util.HadoopConfigProvider;

/**
 * An external shuffle service used by Spark on Yarn.
 *
 * This is intended to be a long-running auxiliary service that runs in the NodeManager process.
 * A Spark application may connect to this service by setting `spark.shuffle.service.enabled`.
 * The application also automatically derives the service port through `spark.shuffle.service.port`
 * specified in the Yarn configuration. This is so that both the clients and the server agree on
 * the same port to communicate on.
 *
 * The service also optionally supports authentication. This ensures that executors from one
 * application cannot read the shuffle files written by those from another. This feature can be
 * enabled by setting `spark.authenticate` in the Yarn configuration before starting the NM.
 * Note that the Spark application must also set `spark.authenticate` manually and, unlike in
 * the case of the service port, will not inherit this setting from the Yarn configuration. This
 * is because an application running on the same Yarn cluster may choose to not use the external
 * shuffle service, in which case its setting of `spark.authenticate` should be independent of
 * the service's.
 */
public class YarnShuffleService extends AbstractService implements AuxServices.AuxiliaryService {
  private final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);

  // Port on which the shuffle server listens for fetch requests
  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  // Whether the shuffle server should authenticate fetch requests
  private static final String SPARK_AUTHENTICATE_KEY = "spark.authenticate";
  private static final boolean DEFAULT_SPARK_AUTHENTICATE = false;

  // An entity that manages the shuffle secret per application
  // This is used only if authentication is enabled
  private ShuffleSecretManager secretManager;

  // The actual server that serves shuffle files
  private TransportServer shuffleServer = null;

  // Handles registering executors and opening shuffle blocks
  private ExternalShuffleBlockHandler blockHandler;

  public YarnShuffleService() {
    super("spark_shuffle");
    logger.info("Initializing YARN shuffle service for Spark");
  }

  /**
   * Return whether authentication is enabled as specified by the configuration.
   * If so, fetch requests will fail unless the appropriate authentication secret
   * for the application is provided.
   */
  private boolean isAuthenticationEnabled() {
    return secretManager != null;
  }

  /**
   * Start the shuffle server with the given configuration.
   */
  @Override
  public synchronized void init(Configuration conf) {
    TransportConf transportConf = new TransportConf(new HadoopConfigProvider(conf));
    // If authentication is enabled, set up the shuffle server to use a
    // special RPC handler that filters out unauthenticated fetch requests
    boolean authEnabled = conf.getBoolean(SPARK_AUTHENTICATE_KEY, DEFAULT_SPARK_AUTHENTICATE);
    blockHandler = new ExternalShuffleBlockHandler(transportConf);
    RpcHandler rpcHandler = blockHandler;
    if (authEnabled) {
      secretManager = new ShuffleSecretManager();
      rpcHandler = new SaslRpcHandler(rpcHandler, secretManager);
    }

    int port = conf.getInt(
      SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
    TransportContext transportContext = new TransportContext(transportConf, rpcHandler);
    shuffleServer = transportContext.createServer(port);
    String authEnabledString = authEnabled ? "enabled" : "not enabled";
    logger.info("Started YARN shuffle service for Spark on port {}. " +
      "Authentication is {}.", port, authEnabledString);
    super.init(new Configuration(conf));
  }

  @Override
  public void initApp(String user, ApplicationId appId, ByteBuffer shuffleSecret) {
    try {
      logger.info("Initializing application {}", appId);
      if (isAuthenticationEnabled()) {
        secretManager.registerApp(appId.toString(), shuffleSecret);
      }
    } catch (Exception e) {
      logger.error("Exception when initializing application {}", appId, e);
    }
  }

  @Override
  public void stopApp(ApplicationId appId) {
    try {
      logger.info("Stopping application {}", appId);
      if (isAuthenticationEnabled()) {
        secretManager.unregisterApp(appId.toString());
      }
      blockHandler.applicationRemoved(appId.toString(), false /* clean up local dirs */);
    } catch (Exception e) {
      logger.error("Exception when stopping application {}", appId, e);
    }
  }


  @Override
  public synchronized void start() {
    super.start();
  }

  /**
   * Close the shuffle server to clean up any associated state.
   */
  @Override
  public synchronized void stop() {
    try {
      if (shuffleServer != null) {
        shuffleServer.close();
      }
    } catch (Exception e) {
      logger.error("Exception when stopping service", e);
    }
    super.stop();
  }

  // Not currently used
  @Override
  public ByteBuffer getMeta() {
    return ByteBuffer.allocate(0);
  }

}
