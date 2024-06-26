/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker.client.impl;

import io.prometheus.client.Gauge;

public class BrokerClientTopologyMetrics {
  private static final Gauge PARTITION_ROLE =
      Gauge.build()
          .namespace("zeebe")
          .name("gateway_topology_partition_roles")
          .help("The partition role of the broker. 0 = Follower, 3 = Leader.")
          .labelNames("partition", "broker")
          .register();

  private static final int FOLLOWER = 0;
  private static final int LEADER = 3;

  public void setLeaderForPartition(final int partition, final int broker) {
    PARTITION_ROLE.labels(String.valueOf(partition), String.valueOf(broker)).set(LEADER);
  }

  public void setFollower(final int partition, final int broker) {
    PARTITION_ROLE.labels(String.valueOf(partition), String.valueOf(broker)).set(FOLLOWER);
  }
}
