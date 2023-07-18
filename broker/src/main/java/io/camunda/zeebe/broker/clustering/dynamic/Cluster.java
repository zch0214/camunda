/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.atomix.cluster.MemberId;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public record Cluster(long version, ClusterState clusterState, ClusterChangePlan changes) {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public byte[] encode() throws JsonProcessingException {
    return objectMapper.writeValueAsBytes(this);
  }

  public Cluster merge(final Cluster newCluster) {
    final var newVersion = Math.max(version, newCluster.version());
    final var newClusterState = clusterState.merge(newCluster.clusterState);
    final var newChanges = changes.merge(newCluster.changes());
    return new Cluster(newVersion, newClusterState, newChanges);
  }

  public static Cluster decode(final byte[] bytes) {
    try {
      return objectMapper.readValue(bytes, Cluster.class);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public record PartitionState(State state, int priority) {}

  public record MemberState(long version, State state, Map<Integer, PartitionState> partitions) {
    MemberState setState(final State newState) {
      return new MemberState(version + 1, newState, partitions);
    }

    MemberState merge(final MemberState other) {
      if (other == null) {
        return this;
      }
      return Collections.max(List.of(this, other), Comparator.comparing(MemberState::version));
    }
  }

  public record ClusterChangeOperation(MemberId memberId, ClusterChangeOperationEnum operation) {}

  public record ClusterChangePlan(long version, List<ClusterChangeOperation> nextSteps) {
    boolean hasPending() {
      return !nextSteps.isEmpty();
    }

    public ClusterChangePlan advance() {
      // TODO: use immutable collections
      final var firstStep = nextSteps.get(0);
      final var steps = nextSteps.stream().filter(item -> !item.equals(firstStep)).toList();
      return new ClusterChangePlan(version, steps);
    }

    public ClusterChangePlan merge(final ClusterChangePlan changes) {
      if (version > changes.version()) {
        return this;
      } else if (changes.version() > version) {
        return changes;
      } else {
        // Take the smallest list : TODO improve this
        return Collections.min(
            List.of(this, changes), Comparator.comparing(l -> l.nextSteps().size()));
      }
    }
  }

  record ClusterState(Map<MemberId, MemberState> members) {
    ClusterState addMember(final MemberId memberId, final MemberState memberState) {
      members.put(memberId, memberState); // TODO make immutable
      return new ClusterState(members);
    }

    ClusterState updateMember(final MemberId memberId, final State state) {
      // TODO:
      return null;
    }

    public ClusterState merge(final ClusterState other) {
      final HashSet<MemberId> memberIds = new HashSet(members.keySet());
      memberIds.addAll(other.members.keySet());

      final HashMap<MemberId, MemberState> newMembers = new HashMap<>();

      memberIds.forEach(
          memberId -> {
            final var thisState = members.get(memberId);
            final var thatState = other.members.get(memberId);
            final MemberState newState;
            if (thisState != null) {
              newState = thisState.merge(thatState);
            } else {
              newState = thatState;
            }
            newMembers.put(memberId, newState);
          });

      return new ClusterState(Map.copyOf(newMembers));
    }
  }

  public enum ClusterChangeOperationEnum {
    JOIN,
    LEAVE
  }

  public enum State {
    UNINITIALIZED,
    JOINING,
    ACTIVE,
    LEAVING,
    LEFT
  }
}
