/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import io.atomix.cluster.MemberId;
// Just to try out immutable collections
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public record Cluster(long version, ClusterState clusterState, ClusterChangePlan changes) {

  public Cluster merge(final Cluster newCluster) {
    // TODO
    return this;
  }

  record ClusterChangePlan() {
    boolean hasPending() {
      // tODO
      return false;
    }
  }

  record ClusterState(Map<MemberId, MemberState> members) {
    ClusterState addMember(final MemberId memberId, final MemberState memberState) {
      return new ClusterState(members.put(memberId, memberState));
    }

    ClusterState updateMember(final MemberId memberId, final State state) {
      final var existingMemberState =
          members.getOrElse(memberId, new MemberState(0, State.UNINITIALIZED, HashMap.empty()));
      return new ClusterState(members.put(memberId, existingMemberState.setState(state)));
    }
  }

  record PartitionState(State state, int priority) {}

  record MemberState(long version, State state, Map<Integer, PartitionState> partitions) {
    MemberState setState(final State newState) {
      return new MemberState(version + 1, newState, partitions);
    }
  }

  private enum State {
    UNINITIALIZED,
    JOINING,
    ACTIVE,
    LEAVING,
    LEFT
  }
}
