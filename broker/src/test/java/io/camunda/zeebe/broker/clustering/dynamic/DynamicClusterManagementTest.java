/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.protocol.SwimMembershipProtocol;
import io.camunda.zeebe.test.util.socket.SocketUtil;
import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DynamicClusterManagementTest {

  @TempDir Path tempDirectory;
  private DynamicClusterAwareNode node0;
  private DynamicClusterAwareNode node1;
  private DynamicClusterAwareNode node2;

  private final List<Closeable> closeables = new ArrayList<>();
  private List<Node> clusterNodes;

  private AtomixCluster createClusterNode(final Node localNode, final Collection<Node> nodes) {
    final var swimProtocol = SwimMembershipProtocol.builder().build();

    final var atomixCluster =
        AtomixCluster.builder()
            .withAddress(localNode.address())
            .withMemberId(localNode.id().id())
            .withMembershipProvider(new BootstrapDiscoveryProvider(nodes))
            .withMembershipProtocol(swimProtocol)
            .build();
    atomixCluster.start().join();
    closeables.add(atomixCluster::stop);
    return atomixCluster;
  }

  @BeforeEach
  void setup() {
    clusterNodes = List.of(createNode("0"), createNode("1"), createNode("2"));

    node0 =
        new DynamicClusterAwareNode(
            MemberId.from("0"),
            null,
            tempDirectory.resolve("0"),
            createClusterNode(clusterNodes.get(0), clusterNodes));
    node1 =
        new DynamicClusterAwareNode(
            MemberId.from("1"),
            null,
            tempDirectory.resolve("1"),
            createClusterNode(clusterNodes.get(1), clusterNodes));
    node2 =
        new DynamicClusterAwareNode(
            MemberId.from("2"),
            null,
            tempDirectory.resolve("2"),
            createClusterNode(clusterNodes.get(2), clusterNodes));
  }

  private Node createNode(final String id) {
    return Node.builder().withId(id).withPort(SocketUtil.getNextAddress().getPort()).build();
  }

  @Test
  void shouldRun() {

    // given
    Awaitility.await()
        .timeout(Duration.ofSeconds(30))
        .until(() -> node0.getConfigManager().isStarted());
    Awaitility.await().until(() -> node1.getConfigManager().isStarted());
    Awaitility.await().until(() -> node2.getConfigManager().isStarted());
    assertThat(node0.getCoordinator().get().getCluster().join().clusterState().members())
        .hasSize(3);

    // when
    final var node3 =
        new DynamicClusterAwareNode(
            MemberId.from("3"),
            null,
            tempDirectory.resolve("3"),
            createClusterNode(createNode("3"), clusterNodes));
    Awaitility.await()
        .timeout(Duration.ofSeconds(30))
        .until(() -> node3.getConfigManager().isStarted());

    node0.getCoordinator().get().addMember(MemberId.from("3"));
    Awaitility.await()
        .timeout(Duration.ofMinutes(1))
        .untilAsserted(
            () ->
                assertThat(
                        node0.getCoordinator().get().getCluster().join().clusterState().members())
                    .hasSize(4));
  }
}
