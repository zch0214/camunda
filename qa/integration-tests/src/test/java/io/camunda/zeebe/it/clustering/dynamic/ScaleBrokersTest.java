/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.clustering.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.qa.util.actuator.ClusterActuator;
import io.camunda.zeebe.qa.util.cluster.TestCluster;
import io.camunda.zeebe.qa.util.cluster.TestStandaloneBroker;
import io.camunda.zeebe.qa.util.cluster.TestZeebePort;
import io.camunda.zeebe.qa.util.junit.ZeebeIntegration;
import io.camunda.zeebe.qa.util.junit.ZeebeIntegration.TestZeebe;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@ZeebeIntegration
class ScaleBrokersTest {

  @TestZeebe
  private static final TestCluster CLUSTER =
      TestCluster.builder()
          .useRecordingExporter(true)
          .withEmbeddedGateway(true)
          .withBrokersCount(2)
          .withPartitionsCount(3)
          .withReplicationFactor(1)
          .withBrokerConfig(
              b ->
                  b.brokerConfig()
                      .getExperimental()
                      .getFeatures()
                      .setEnableDynamicClusterTopology(true))
          .build();

  @Test
  void shouldScaleBrokers() {
    // given
    final int currentClusterSize = CLUSTER.brokers().size();
    final int newClusterSize = currentClusterSize + 1;
    try (final var newBroker =
        new TestStandaloneBroker()
            .withBrokerConfig(
                b -> {
                  b.getExperimental().getFeatures().setEnableDynamicClusterTopology(true);
                  b.getCluster().setClusterSize(newClusterSize); // TO verify
                  b.getCluster().setNodeId(newClusterSize - 1);
                  b.getCluster()
                      .setInitialContactPoints(
                          List.of(
                              CLUSTER
                                  .brokers()
                                  .get(MemberId.from("0"))
                                  .address(TestZeebePort.CLUSTER)));
                })
            .start()) {

      final var actuator = ClusterActuator.of(CLUSTER.availableGateway());
      final var newBrokerSet = IntStream.range(0, currentClusterSize + 1).boxed().toList();

      // when
      actuator.scaleBrokers(newBrokerSet);

      // then
      Awaitility.await()
          .timeout(Duration.ofMinutes(2))
          .untilAsserted(
              () ->
                  assertThat(actuator.getTopology().getBrokers())
                      .describedAs("Partition 3 is moved to the new broker")
                      .hasSize(newClusterSize)
                      .allSatisfy(b -> assertThat(b.getPartitions()).hasSize(1)));

      // Changes are reflected in the topology returned by grpc query
      CLUSTER.awaitCompleteTopology(newClusterSize, 3, 1, Duration.ofSeconds(10));
    }
  }
}
