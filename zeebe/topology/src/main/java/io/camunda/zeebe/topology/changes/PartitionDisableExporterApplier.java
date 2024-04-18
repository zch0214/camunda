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
import io.camunda.zeebe.topology.changes.TopologyChangeAppliers.MemberOperationApplier;
import io.camunda.zeebe.topology.state.ClusterTopology;
import io.camunda.zeebe.topology.state.MemberState;
import io.camunda.zeebe.util.Either;
import java.util.function.UnaryOperator;

public class PartitionDisableExporterApplier implements MemberOperationApplier {

  private final int partitionId;
  private final MemberId memberId;
  private final String exporterId;
  private final PartitionChangeExecutor partitionChangeExecutor;

  public PartitionDisableExporterApplier(
      final int partitionId,
      final MemberId memberId,
      final String exporterId,
      final PartitionChangeExecutor partitionChangeExecutor) {
    this.partitionId = partitionId;
    this.memberId = memberId;
    this.exporterId = exporterId;
    this.partitionChangeExecutor = partitionChangeExecutor;
  }

  @Override
  public MemberId memberId() {
    return memberId;
  }

  @Override
  public Either<Exception, UnaryOperator<MemberState>> initMemberState(
      final ClusterTopology currentClusterTopology) {
    // TODO : validate preconditions
    return Either.right(m -> m);
  }

  @Override
  public ActorFuture<UnaryOperator<MemberState>> applyOperation() {
    final CompletableActorFuture<UnaryOperator<MemberState>> result =
        new CompletableActorFuture<>();
    partitionChangeExecutor
        .disableExporter(partitionId, exporterId)
        .onComplete(
            (ok, error) -> {
              if (error == null) {
                result.complete(
                    m ->
                        m.updatePartition(
                            partitionId, p -> p.updateConfig(c -> c.disableExporter(exporterId))));
              } else {
                result.completeExceptionally(error);
              }
            });
    return result;
  }
}
