/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonParseException;
import io.atomix.cluster.MemberId;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.scheduler.testing.TestConcurrencyControl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ClusterTopologyManagerTest {
  private final ControllableGossipHandler gossipHandler = new ControllableGossipHandler();

  @Test
  void shouldInitializeClusterTopologyFromBrokerCfg(@TempDir final Path topologyFile) {
    // given
    final ClusterTopologyManager clusterTopologyManager =
        new ClusterTopologyManager(
            new TestConcurrencyControl(),
            new PersistedClusterTopology(topologyFile.resolve("topology.temp")),
            gossipHandler);
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setClusterSize(3);
    brokerCfg.getCluster().setPartitionsCount(3);
    brokerCfg.getCluster().setReplicationFactor(1);

    // when
    clusterTopologyManager.start(brokerCfg).join();

    // then
    final ClusterTopology clusterTopology = clusterTopologyManager.getClusterTopology().join();
    ClusterTopologyAssert.assertThatClusterTopology(clusterTopology)
        .hasMemberWithPartitions(0, Set.of(1))
        .hasMemberWithPartitions(1, Set.of(2))
        .hasMemberWithPartitions(2, Set.of(3));
  }

  @Test
  void shouldGossipInitialConfiguration(@TempDir final Path topologyFile) {
    // given
    final ClusterTopologyManager clusterTopologyManager =
        new ClusterTopologyManager(
            new TestConcurrencyControl(),
            new PersistedClusterTopology(topologyFile.resolve("topology.temp")),
            gossipHandler);
    final BrokerCfg brokerCfg = new BrokerCfg();

    // when
    clusterTopologyManager.start(brokerCfg).join();

    // then
    Awaitility.await().until(() -> gossipHandler.topologyToGossip != null);

    final ClusterTopology gossippedTopology = gossipHandler.topologyToGossip;
    final ClusterTopology actualTopology = clusterTopologyManager.getClusterTopology().join();
    assertThat(gossippedTopology).isEqualTo(actualTopology);
  }

  @Test
  void shouldInitializeClusterTopologyFromFile(@TempDir final Path topologyFile)
      throws IOException {
    // given
    final Path existingTopologyFile = topologyFile.resolve("topology.temp");
    final var existingTopology =
        ClusterTopology.init()
            .addMember(
                MemberId.from("5"),
                MemberState.initializeAsActive(Map.of(10, PartitionState.active(4))));
    Files.write(existingTopologyFile, existingTopology.encode());
    final ClusterTopologyManager clusterTopologyManager =
        new ClusterTopologyManager(
            new TestConcurrencyControl(),
            new PersistedClusterTopology(existingTopologyFile),
            gossipHandler);

    // when
    clusterTopologyManager.start(new BrokerCfg()).join();

    // then
    final ClusterTopology clusterTopology = clusterTopologyManager.getClusterTopology().join();
    ClusterTopologyAssert.assertThatClusterTopology(clusterTopology)
        .hasOnlyMembers(Set.of(5))
        .hasMemberWithPartitions(5, Set.of(10));
  }

  @Test
  void shouldFailIfTopologyFileIsCorrupted(@TempDir final Path topologyFile) throws IOException {
    // given
    final Path existingTopologyFile = topologyFile.resolve("topology.temp");
    Files.write(existingTopologyFile, new byte[10]); // write random string
    final ClusterTopologyManager clusterTopologyManager =
        new ClusterTopologyManager(
            new TestConcurrencyControl(),
            new PersistedClusterTopology(existingTopologyFile),
            gossipHandler);

    // when - then
    assertThat(clusterTopologyManager.start(new BrokerCfg()))
        .failsWithin(Duration.ofMillis(100))
        .withThrowableThat()
        .withCauseInstanceOf(JsonParseException.class);
  }

  @Test
  void shouldUpdateLocalTopologyOnGossipEvent(@TempDir final Path topologyFile) {
    // given
    final ClusterTopologyManager clusterTopologyManager =
        new ClusterTopologyManager(
            new TestConcurrencyControl(),
            new PersistedClusterTopology(topologyFile.resolve("topology.temp")),
            gossipHandler);
    final BrokerCfg brokerCfg = new BrokerCfg();
    clusterTopologyManager.start(brokerCfg).join();

    // when
    final ClusterTopology topologyFromOtherMember =
        ClusterTopology.init()
            .addMember(MemberId.from("1"), MemberState.initializeAsActive(Map.of()));
    gossipHandler.pushClusterTopology(topologyFromOtherMember);

    // then
    final ClusterTopology clusterTopology = clusterTopologyManager.getClusterTopology().join();
    ClusterTopologyAssert.assertThatClusterTopology(clusterTopology).hasOnlyMembers(Set.of(0, 1));
  }

  private static class ControllableGossipHandler implements ClusterTopologyGossipHandler {

    ClusterTopology topologyToGossip;
    private Consumer<ClusterTopology> clusterTopologyConsumer;

    @Override
    public void gossip(final ClusterTopology clusterTopology) {
      topologyToGossip = clusterTopology;
    }

    @Override
    public void registerListener(final Consumer<ClusterTopology> clusterTopologyConsumer) {
      this.clusterTopologyConsumer = clusterTopologyConsumer;
    }

    void pushClusterTopology(final ClusterTopology topologyFromOtherMember) {
      clusterTopologyConsumer.accept(topologyFromOtherMember);
    }
  }
}
