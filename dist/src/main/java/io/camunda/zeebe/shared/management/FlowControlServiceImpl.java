/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.shared.management;

import com.netflix.concurrency.limits.Limit;
import io.camunda.zeebe.broker.client.api.BrokerClient;
import io.camunda.zeebe.broker.client.api.BrokerClusterState;
import io.camunda.zeebe.broker.system.configuration.FlowControlCfg;
import io.camunda.zeebe.gateway.admin.BrokerAdminRequest;
import io.camunda.zeebe.logstreams.impl.flowcontrol.LimitType;
import io.camunda.zeebe.shared.management.FlowControlEndpoint.FlowControlService;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.agrona.collections.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FlowControlServiceImpl implements FlowControlService {
  private static final Logger LOG = LoggerFactory.getLogger(FlowControlServiceImpl.class);
  private final BrokerClient client;

  @Autowired
  public FlowControlServiceImpl(final BrokerClient client) {
    this.client = client;
  }

  @Override
  public CompletableFuture<Map<Integer, Map<LimitType, Limit>>> get() {
    LOG.info("Fetching flow control configuration.");
    final var topology = client.getTopologyManager().getTopology();

    final var requests =
        topology.getPartitions().stream()
            .map(
                partition ->
                    fetchFlowConfigOnPartition(
                        topology, partition, BrokerAdminRequest::getFLowControlConfiguration))
            .toArray(CompletableFuture<?>[]::new);

    Arrays.stream(requests).map(CompletableFuture::join);

    return new CompletableFuture<Map<Integer, Map<LimitType, Limit>>>();
  }

  @Override
  public CompletableFuture<Void> set(final FlowControlCfg flowControlCfg) {
    LOG.info("Setting flow control configuration.");
    final var topology = client.getTopologyManager().getTopology();
    final var requests =
        topology.getPartitions().stream()
            .map(
                partition ->
                    broadcastOnPartition(
                        topology,
                        partition,
                        request -> {
                          request.setFlowControlConfiguration(null, 0, 0);
                        }))
            .toArray(CompletableFuture<?>[]::new);
    return CompletableFuture.allOf(requests);
  }

  private CompletableFuture<Void> broadcastOnPartition(
      final BrokerClusterState topology,
      final Integer partitionId,
      final Consumer<BrokerAdminRequest> configureRequest) {

    final var leader = topology.getLeaderForPartition(partitionId);
    final var followers =
        Optional.ofNullable(topology.getFollowersForPartition(partitionId)).orElseGet(Set::of);
    final var inactive =
        Optional.ofNullable(topology.getInactiveNodesForPartition(partitionId)).orElseGet(Set::of);

    final var members = new IntHashSet(topology.getReplicationFactor());
    members.add(leader);
    members.addAll(followers);
    members.addAll(inactive);

    final var requests =
        members.stream()
            .map(
                brokerId -> {
                  final var request = new BrokerAdminRequest();
                  request.setBrokerId(brokerId);
                  request.setPartitionId(partitionId);
                  configureRequest.accept(request);
                  return client.sendRequest(request);
                })
            .toArray(CompletableFuture<?>[]::new);
    return CompletableFuture.allOf(requests);
  }

  private CompletableFuture<Void> fetchFlowConfigOnPartition(
      final BrokerClusterState topology,
      final Integer partitionId,
      final Consumer<BrokerAdminRequest> configureRequest) {

    final var leader = topology.getLeaderForPartition(partitionId);
    final var followers =
        Optional.ofNullable(topology.getFollowersForPartition(partitionId)).orElseGet(Set::of);
    final var inactive =
        Optional.ofNullable(topology.getInactiveNodesForPartition(partitionId)).orElseGet(Set::of);

    final var members = new IntHashSet(topology.getReplicationFactor());
    members.add(leader);
    members.addAll(followers);
    members.addAll(inactive);

    final var requests =
        members.stream()
            .map(
                brokerId -> {
                  final var request = new BrokerAdminRequest();
                  request.setBrokerId(brokerId);
                  request.setPartitionId(partitionId);
                  request.getFLowControlConfiguration();
                  final var brokerResponseCompletableFuture = client.sendRequest(request);
                  final var response = brokerResponseCompletableFuture.join().getResponse();
                  return brokerResponseCompletableFuture;
                })
            .toArray(CompletableFuture<?>[]::new);
    return CompletableFuture.allOf(requests);
  }
}
