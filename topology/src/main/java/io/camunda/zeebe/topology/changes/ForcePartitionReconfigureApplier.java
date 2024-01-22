/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.changes;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.topology.changes.TopologyChangeAppliers.OperationApplier;
import io.camunda.zeebe.topology.state.ClusterTopology;
import io.camunda.zeebe.topology.state.MemberState;
import io.camunda.zeebe.topology.state.PartitionState;
import io.camunda.zeebe.util.Either;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class ForcePartitionReconfigureApplier implements OperationApplier {
  private final int partitionId;
  private final MemberId localMemberId;
  private final PartitionChangeExecutor partitionChangeExecutor;
  private Map<MemberId, Integer> partitionMembersWithPriority;

  public ForcePartitionReconfigureApplier(
      final int partitionId,
      final MemberId localMemberId,
      final PartitionChangeExecutor partitionChangeExecutor) {
    this.partitionId = partitionId;
    this.localMemberId = localMemberId;
    this.partitionChangeExecutor = partitionChangeExecutor;
  }

  @Override
  public Either<Exception, UnaryOperator<MemberState>> init(
      final ClusterTopology currentClusterTopology) {
    partitionMembersWithPriority = collectPriorityByMembers(currentClusterTopology);
    return Either.right(memberState -> memberState);
  }

  @Override
  public ActorFuture<UnaryOperator<MemberState>> apply() {
    final CompletableActorFuture<UnaryOperator<MemberState>> result =
        new CompletableActorFuture<>();

    partitionChangeExecutor
        .reconfigurePartition(partitionId, partitionMembersWithPriority)
        .onComplete(
            (ignore, error) -> {
              if (error == null) {
                result.complete(
                    memberState ->
                        memberState.updatePartition(partitionId, PartitionState::toActive));
              } else {
                result.completeExceptionally(error);
              }
            });
    return result;
  }

  private HashMap<MemberId, Integer> collectPriorityByMembers(
      final ClusterTopology currentClusterTopology) {
    final var priorityMap = new HashMap<MemberId, Integer>();
    currentClusterTopology
        .members()
        .forEach(
            (memberId, memberState) -> {
              if (memberState.partitions().containsKey(partitionId)) {
                final var partitionState = memberState.partitions().get(partitionId);
                priorityMap.put(memberId, partitionState.priority());
              }
            });
    return priorityMap;
  }
}
