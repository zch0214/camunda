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
import io.camunda.zeebe.topology.changes.TopologyChangeAppliers.OperationApplier;
import io.camunda.zeebe.topology.state.ClusterTopology;
import io.camunda.zeebe.topology.state.MemberState;
import io.camunda.zeebe.util.Either;
import java.util.List;
import java.util.function.UnaryOperator;

// Doesn't work because this operation should overwrite multiple members which is not allowed by
// this interface
public class PartitionOverwriteConfigurationApplier implements OperationApplier {
  public PartitionOverwriteConfigurationApplier(
      final int partitionId,
      final List<MemberId> newMembers,
      final MemberId memberId,
      final PartitionChangeExecutor partitionChangeExecutor) {}

  @Override
  public Either<Exception, UnaryOperator<MemberState>> init(
      final ClusterTopology currentClusterTopology) {
    return null;
  }

  @Override
  public ActorFuture<UnaryOperator<MemberState>> apply() {

    return null;
  }
}
