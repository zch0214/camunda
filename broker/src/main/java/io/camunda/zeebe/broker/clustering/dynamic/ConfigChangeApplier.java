/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.ClusterChangePlan;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.MemberState;
import io.camunda.zeebe.broker.clustering.dynamic.Cluster.State;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigChangeApplier {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigChangeApplier.class);
  private final MemberId localMemberId;

  public ConfigChangeApplier(final MemberId localMemberId) {
    this.localMemberId = localMemberId;
  }

  public void apply(
      final ClusterChangePlan changes, final Consumer<UnaryOperator<Cluster>> clusterUpdater) {
    final var nextStep = changes.nextSteps().get(0);
    if (!nextStep.memberId().equals(localMemberId)) {
      return;
    }

    switch (nextStep.operation()) {
      case JOIN -> {
        LOG.info("Member {} joining cluster.", localMemberId);
        LOG.info("Member {} joined cluster. Updating config", localMemberId);
        final var nextChanges = changes.advance();
        clusterUpdater.accept(
            cluster -> updateConfigOnMemberJoined(cluster, localMemberId, nextChanges));
      }

      case LEAVE -> {
        LOG.info("Member {} leaving cluster. Updating config", localMemberId);
        final var nextChanges = changes.advance();
        clusterUpdater.accept(
            cluster -> updateConfigOnMemberLeft(cluster, localMemberId, nextChanges));
      }
    }
  }

  private Cluster updateConfigOnMemberJoined(
      final Cluster cluster, final MemberId memberId, final ClusterChangePlan changes) {
    final var newClusterState =
        cluster.clusterState().addMember(memberId, new MemberState(0, State.ACTIVE, Map.of()));
    return new Cluster(cluster.version(), newClusterState, changes);
  }

  private Cluster updateConfigOnMemberLeft(
      final Cluster cluster, final MemberId memberId, final ClusterChangePlan changes) {
    final var newClusterState =
        cluster.clusterState().addMember(memberId, new MemberState(0, State.LEFT, Map.of()));
    return new Cluster(cluster.version(), newClusterState, changes);
  }
}
