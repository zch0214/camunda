/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.topology;

import java.util.function.Consumer;

public interface ClusterTopologyGossipHandler {

  /**
   * Eventually propagates clusterTopology to other members.
   *
   * @param clusterTopology topology to propagate
   */
  void gossip(ClusterTopology clusterTopology);

  /**
   * Registers a listener which will notified when potential update to ClusterTopology is received.
   * Note that the listener can still be invoked if the received topology is identical to the local
   * one.
   *
   * @param clusterTopologyConsumer listener that will be notified when
   */
  void registerListener(Consumer<ClusterTopology> clusterTopologyConsumer);
}
