/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.operate.zeebeimport.severalversions;

import java.time.Duration;
import org.apache.http.HttpHost;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;
import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import io.zeebe.containers.ZeebePort;

public class TestContainerUtil {

  private static final Logger logger = LoggerFactory.getLogger(TestContainerUtil.class);

  private static final String DOCKER_ELASTICSEARCH_IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch-oss";
  public static final String ELS_NETWORK_ALIAS = "elasticsearch";
  public static final int ELS_PORT = 9200;
  public static final String ZEEBE_CFG_TOML_FILE = "/severalversions/zeebe.cfg.toml";
  public static final String ZEEBE_CFG_YAML_FILE = "/severalversions/zeebe.cfg.yml";
  public static final String PROPERTIES_PREFIX = "camunda.operate.";

  private Network network;
  private ZeebeBrokerContainer broker;
  private ElasticsearchContainer elsContainer;
  private ZeebeClient client;

  private String contactPoint;
  private String elsHost;
  private Integer elsPort;
  private RestHighLevelClient esClient;

  public void startZeebe(final String dataFolderPath, final String version) {
    broker = new ZeebeBrokerContainer(version)
        .withFileSystemBind(dataFolderPath, "/usr/local/zeebe/data")
        .withNetwork(getNetwork())
        .withEmbeddedGateway(true)
        .withLogLevel(Level.DEBUG);
    addConfig(broker, version);
    broker.start();

    contactPoint = broker.getExternalAddress(ZeebePort.GATEWAY);
    client = ZeebeClient.newClientBuilder().brokerContactPoint(contactPoint).usePlaintext().build();
  }

  private void addConfig(ZeebeBrokerContainer broker, String version) {
    boolean tomlVersion = false;
    final String[] versionParts = version.split("\\.");
    try {
      if (versionParts.length >= 2 && Integer.valueOf(versionParts[1]) <= 22) {
        tomlVersion = true;
      }
    } catch (NumberFormatException ex) {
      //second part of version is not a number, skipping
    }
    if (tomlVersion) {
      broker.withConfigurationResource(ZEEBE_CFG_TOML_FILE);
    } else {
      broker.withCopyFileToContainer(MountableFile.forClasspathResource(ZEEBE_CFG_YAML_FILE),
          "/usr/local/zeebe/config/application.yml");
    }
  }

  public void startElasticsearch() {
    elsContainer = new ElasticsearchContainer(String.format("%s:%s", DOCKER_ELASTICSEARCH_IMAGE_NAME, ElasticsearchClient.class.getPackage().getImplementationVersion()))
        .withNetwork(getNetwork())
        .withNetworkAliases(ELS_NETWORK_ALIAS)
        .withExposedPorts(ELS_PORT);
    elsContainer.start();
    elsHost = elsContainer.getContainerIpAddress();
    elsPort = elsContainer.getMappedPort(ELS_PORT);
    logger.info(String.format("Elasticsearch started on %s:%s", elsHost, elsPort));
  }

  public String[] getOperateProperties() {
    return new String[] {
        PROPERTIES_PREFIX + "elasticsearch.host=" + elsHost,
        PROPERTIES_PREFIX + "elasticsearch.port=" + elsPort,
        PROPERTIES_PREFIX + "zeebeElasticsearch.host=" + elsHost,
        PROPERTIES_PREFIX + "zeebeElasticsearch.port=" + elsPort,
        PROPERTIES_PREFIX + "zeebeElasticsearch.prefix=" + ImportSeveralVersionsInitializer.ZEEBE_PREFIX,
        PROPERTIES_PREFIX + "zeebe.brokerContactPoint=" + contactPoint,
        PROPERTIES_PREFIX + "importer.startLoadingDataOnStartup=false"
    };
  }

  public RestHighLevelClient getEsClient() {
    if (esClient == null) {
      esClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elsHost, elsPort, "http")));
    }
    return esClient;
  }

  private Network getNetwork() {
    if (network == null) {
      network = Network.newNetwork();
    }
    return network;
  }

  public void stopAll() {
    stopZeebe();
    stopEls();
    closeNetwork();
  }

  public void stopZeebe() {
    if (client != null) {
      client.close();
      client = null;
    }
    if (broker != null) {
      broker.shutdownGracefully(Duration.ofSeconds(3));
      broker = null;
    }
  }

  private void stopEls() {
    if (elsContainer != null) {
      elsContainer.stop();
    }
  }

  private void closeNetwork(){
    if (network != null) {
      network.close();
      network = null;
    }
  }

  public ZeebeClient getClient() {
    return client;
  }

}