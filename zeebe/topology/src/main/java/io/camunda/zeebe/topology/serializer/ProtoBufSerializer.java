/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.atomix.cluster.MemberId;
import io.camunda.zeebe.topology.api.ErrorResponse;
import io.camunda.zeebe.topology.api.TopologyChangeResponse;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.AddMembersRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.CancelChangeRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.DisableExporterRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.EnableExporterRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.JoinPartitionRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.LeavePartitionRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.ReassignPartitionsRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.RemoveMembersRequest;
import io.camunda.zeebe.topology.api.TopologyManagementRequest.ScaleRequest;
import io.camunda.zeebe.topology.gossip.ClusterTopologyGossipState;
import io.camunda.zeebe.topology.protocol.Requests;
import io.camunda.zeebe.topology.protocol.Requests.CancelTopologyChangeRequest;
import io.camunda.zeebe.topology.protocol.Requests.ErrorCode;
import io.camunda.zeebe.topology.protocol.Requests.ReassignAllPartitionsRequest;
import io.camunda.zeebe.topology.protocol.Requests.Response;
import io.camunda.zeebe.topology.protocol.Requests.TopologyChangeResponse.Builder;
import io.camunda.zeebe.topology.protocol.Topology;
import io.camunda.zeebe.topology.protocol.Topology.ChangeStatus;
import io.camunda.zeebe.topology.protocol.Topology.CompletedChange;
import io.camunda.zeebe.topology.protocol.Topology.CompletedTopologyChangeOperation;
import io.camunda.zeebe.topology.protocol.Topology.ExporterConfig;
import io.camunda.zeebe.topology.protocol.Topology.ExporterState;
import io.camunda.zeebe.topology.protocol.Topology.GossipState;
import io.camunda.zeebe.topology.protocol.Topology.MemberState;
import io.camunda.zeebe.topology.protocol.Topology.State;
import io.camunda.zeebe.topology.state.ClusterChangePlan;
import io.camunda.zeebe.topology.state.ClusterChangePlan.CompletedOperation;
import io.camunda.zeebe.topology.state.ClusterChangePlan.Status;
import io.camunda.zeebe.topology.state.ClusterTopology;
import io.camunda.zeebe.topology.state.DynamicConfiguration;
import io.camunda.zeebe.topology.state.PartitionState;
import io.camunda.zeebe.topology.state.TopologyChangeOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.MemberJoinOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.MemberLeaveOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.MemberRemoveOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionDisableExporterOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionEnableExporterOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionForceReconfigureOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionJoinOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionLeaveOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionReconfigurePriorityOperation;
import io.camunda.zeebe.util.Either;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

public class ProtoBufSerializer implements ClusterTopologySerializer, TopologyRequestsSerializer {

  @Override
  public byte[] encode(final ClusterTopologyGossipState gossipState) {
    final var builder = GossipState.newBuilder();

    final ClusterTopology topologyToEncode = gossipState.getClusterTopology();
    if (topologyToEncode != null) {
      final Topology.ClusterTopology clusterTopology = encodeClusterTopology(topologyToEncode);
      builder.setClusterTopology(clusterTopology);
    }

    final var message = builder.build();
    return message.toByteArray();
  }

  @Override
  public ClusterTopologyGossipState decode(final byte[] encodedState) {
    final GossipState gossipState;

    try {
      gossipState = GossipState.parseFrom(encodedState);
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
    final ClusterTopologyGossipState clusterTopologyGossipState = new ClusterTopologyGossipState();

    if (gossipState.hasClusterTopology()) {
      try {
        clusterTopologyGossipState.setClusterTopology(
            decodeClusterTopology(gossipState.getClusterTopology()));
      } catch (final Exception e) {
        throw new DecodingFailed(
            "Cluster topology could not be deserialized from gossiped state: %s"
                .formatted(gossipState),
            e);
      }
    }
    return clusterTopologyGossipState;
  }

  @Override
  public byte[] encode(final ClusterTopology clusterTopology) {
    return encodeClusterTopology(clusterTopology).toByteArray();
  }

  @Override
  public ClusterTopology decodeClusterTopology(
      final byte[] encodedClusterTopology, final int offset, final int length) {
    try {
      final var topology =
          Topology.ClusterTopology.parseFrom(
              ByteBuffer.wrap(encodedClusterTopology, offset, length));
      return decodeClusterTopology(topology);

    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  private ClusterTopology decodeClusterTopology(
      final Topology.ClusterTopology encodedClusterTopology) {

    final var members = decodeMemberStateMap(encodedClusterTopology.getMembersMap());

    final Optional<io.camunda.zeebe.topology.state.CompletedChange> completedChange =
        encodedClusterTopology.hasLastChange()
            ? Optional.of(decodeCompletedChange(encodedClusterTopology.getLastChange()))
            : Optional.empty();
    final Optional<ClusterChangePlan> currentChange =
        encodedClusterTopology.hasCurrentChange()
            ? Optional.of(decodeChangePlan(encodedClusterTopology.getCurrentChange()))
            : Optional.empty();

    return new ClusterTopology(
        encodedClusterTopology.getVersion(), members, completedChange, currentChange);
  }

  private Map<MemberId, io.camunda.zeebe.topology.state.MemberState> decodeMemberStateMap(
      final Map<String, MemberState> membersMap) {
    return membersMap.entrySet().stream()
        .map(e -> Map.entry(MemberId.from(e.getKey()), decodeMemberState(e.getValue())))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  private Topology.ClusterTopology encodeClusterTopology(final ClusterTopology clusterTopology) {
    final var members = encodeMemberStateMap(clusterTopology.members());

    final var builder =
        Topology.ClusterTopology.newBuilder()
            .setVersion(clusterTopology.version())
            .putAllMembers(members);

    clusterTopology
        .lastChange()
        .ifPresent(lastChange -> builder.setLastChange(encodeCompletedChange(lastChange)));
    clusterTopology
        .pendingChanges()
        .ifPresent(changePlan -> builder.setCurrentChange(encodeChangePlan(changePlan)));

    return builder.build();
  }

  private io.camunda.zeebe.topology.state.MemberState decodeMemberState(
      final MemberState memberState) {
    final var partitions =
        memberState.getPartitionsMap().entrySet().stream()
            .map(e -> Map.entry(e.getKey(), decodePartitionState(e.getValue())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    final Timestamp lastUpdated = memberState.getLastUpdated();
    return new io.camunda.zeebe.topology.state.MemberState(
        memberState.getVersion(),
        Instant.ofEpochSecond(lastUpdated.getSeconds(), lastUpdated.getNanos()),
        toMemberState(memberState.getState()),
        partitions);
  }

  private PartitionState decodePartitionState(final Topology.PartitionState partitionState) {
    return new PartitionState(
        toPartitionState(partitionState.getState()),
        partitionState.getPriority(),
        decodeDynamicConfig(partitionState.getConfig()));
  }

  private MemberState encodeMemberState(
      final io.camunda.zeebe.topology.state.MemberState memberState) {
    final var partitions =
        memberState.partitions().entrySet().stream()
            .map(e -> Map.entry(e.getKey(), encodePartitions(e.getValue())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    final Instant lastUpdated = memberState.lastUpdated();
    return MemberState.newBuilder()
        .setVersion(memberState.version())
        .setLastUpdated(
            Timestamp.newBuilder()
                .setSeconds(lastUpdated.getEpochSecond())
                .setNanos(lastUpdated.getNano())
                .build())
        .setState(toSerializedState(memberState.state()))
        .putAllPartitions(partitions)
        .build();
  }

  private Topology.PartitionState encodePartitions(final PartitionState partitionState) {
    return Topology.PartitionState.newBuilder()
        .setState(toSerializedState(partitionState.state()))
        .setPriority(partitionState.priority())
        .setConfig(encodeDynamicConfiguration(partitionState.config()))
        .build();
  }

  private State toSerializedState(final io.camunda.zeebe.topology.state.MemberState.State state) {
    return switch (state) {
      case UNINITIALIZED -> State.UNKNOWN;
      case ACTIVE -> State.ACTIVE;
      case JOINING -> State.JOINING;
      case LEAVING -> State.LEAVING;
      case LEFT -> State.LEFT;
    };
  }

  private io.camunda.zeebe.topology.state.MemberState.State toMemberState(final State state) {
    return switch (state) {
      case UNRECOGNIZED, UNKNOWN -> io.camunda.zeebe.topology.state.MemberState.State.UNINITIALIZED;
      case ACTIVE -> io.camunda.zeebe.topology.state.MemberState.State.ACTIVE;
      case JOINING -> io.camunda.zeebe.topology.state.MemberState.State.JOINING;
      case LEAVING -> io.camunda.zeebe.topology.state.MemberState.State.LEAVING;
      case LEFT -> io.camunda.zeebe.topology.state.MemberState.State.LEFT;
    };
  }

  private PartitionState.State toPartitionState(final State state) {
    return switch (state) {
      case UNRECOGNIZED, UNKNOWN, LEFT -> PartitionState.State.UNKNOWN;
      case ACTIVE -> PartitionState.State.ACTIVE;
      case JOINING -> PartitionState.State.JOINING;
      case LEAVING -> PartitionState.State.LEAVING;
    };
  }

  private State toSerializedState(final PartitionState.State state) {
    return switch (state) {
      case UNKNOWN -> State.UNKNOWN;
      case ACTIVE -> State.ACTIVE;
      case JOINING -> State.JOINING;
      case LEAVING -> State.LEAVING;
    };
  }

  private Topology.ClusterChangePlan encodeChangePlan(final ClusterChangePlan changes) {
    final var builder =
        Topology.ClusterChangePlan.newBuilder()
            .setVersion(changes.version())
            .setId(changes.id())
            .setStatus(fromTopologyChangeStatus(changes.status()))
            .setStartedAt(
                Timestamp.newBuilder()
                    .setSeconds(changes.startedAt().getEpochSecond())
                    .setNanos(changes.startedAt().getNano())
                    .build());
    changes
        .pendingOperations()
        .forEach(operation -> builder.addPendingOperations(encodeOperation(operation)));
    changes
        .completedOperations()
        .forEach(operation -> builder.addCompletedOperations(encodeCompletedOperation(operation)));

    return builder.build();
  }

  private CompletedChange encodeCompletedChange(
      final io.camunda.zeebe.topology.state.CompletedChange completedChange) {
    final var builder = CompletedChange.newBuilder();
    builder
        .setId(completedChange.id())
        .setStatus(fromTopologyChangeStatus(completedChange.status()))
        .setCompletedAt(
            Timestamp.newBuilder()
                .setSeconds(completedChange.completedAt().getEpochSecond())
                .setNanos(completedChange.completedAt().getNano())
                .build())
        .setStartedAt(
            Timestamp.newBuilder()
                .setSeconds(completedChange.startedAt().getEpochSecond())
                .setNanos(completedChange.startedAt().getNano())
                .build());

    return builder.build();
  }

  private Topology.TopologyChangeOperation encodeOperation(
      final TopologyChangeOperation operation) {
    final var builder =
        Topology.TopologyChangeOperation.newBuilder().setMemberId(operation.memberId().id());
    switch (operation) {
      case final PartitionJoinOperation joinOperation ->
          builder.setPartitionJoin(
              Topology.PartitionJoinOperation.newBuilder()
                  .setPartitionId(joinOperation.partitionId())
                  .setPriority(joinOperation.priority()));
      case final PartitionLeaveOperation leaveOperation ->
          builder.setPartitionLeave(
              Topology.PartitionLeaveOperation.newBuilder()
                  .setPartitionId(leaveOperation.partitionId()));
      case final MemberJoinOperation memberJoinOperation ->
          builder.setMemberJoin(Topology.MemberJoinOperation.newBuilder().build());
      case final MemberLeaveOperation memberLeaveOperation ->
          builder.setMemberLeave(Topology.MemberLeaveOperation.newBuilder().build());
      case final PartitionReconfigurePriorityOperation reconfigurePriorityOperation ->
          builder.setPartitionReconfigurePriority(
              Topology.PartitionReconfigurePriorityOperation.newBuilder()
                  .setPartitionId(reconfigurePriorityOperation.partitionId())
                  .setPriority(reconfigurePriorityOperation.priority())
                  .build());
      case final PartitionForceReconfigureOperation forceReconfigureOperation ->
          builder.setPartitionForceReconfigure(
              Topology.PartitionForceReconfigureOperation.newBuilder()
                  .setPartitionId(forceReconfigureOperation.partitionId())
                  .addAllMembers(
                      forceReconfigureOperation.members().stream().map(MemberId::id).toList())
                  .build());
      case final MemberRemoveOperation memberRemoveOperation ->
          builder.setMemberRemove(
              Topology.MemberRemoveOperation.newBuilder()
                  .setMemberToRemove(memberRemoveOperation.memberToRemove().id())
                  .build());
      case final PartitionDisableExporterOperation disableExporterOperation ->
          builder.setPartitionExporterDisable(
              Topology.PartitionExporterDisableOperation.newBuilder()
                  .setPartitionId(disableExporterOperation.partitionId())
                  .setExporterId(disableExporterOperation.exporterId())
                  .build());
      case final PartitionEnableExporterOperation enableExporterOperation ->
          builder.setPartitionExporterEnable(
              Topology.PartitionExporterEnableOperation.newBuilder()
                  .setPartitionId(enableExporterOperation.partitionId())
                  .setExporterId(enableExporterOperation.exporterId())
                  .build());
      default ->
          throw new IllegalArgumentException(
              "Unknown operation type: " + operation.getClass().getSimpleName());
    }
    return builder.build();
  }

  private CompletedTopologyChangeOperation encodeCompletedOperation(
      final CompletedOperation completedOperation) {
    return CompletedTopologyChangeOperation.newBuilder()
        .setOperation(encodeOperation(completedOperation.operation()))
        .setCompletedAt(
            Timestamp.newBuilder()
                .setSeconds(completedOperation.completedAt().getEpochSecond())
                .setNanos(completedOperation.completedAt().getNano())
                .build())
        .build();
  }

  private ClusterChangePlan decodeChangePlan(final Topology.ClusterChangePlan clusterChangePlan) {

    final var version = clusterChangePlan.getVersion();
    final var pendingOperations =
        clusterChangePlan.getPendingOperationsList().stream()
            .map(this::decodeOperation)
            .collect(Collectors.toList());
    final var completedOperations =
        clusterChangePlan.getCompletedOperationsList().stream()
            .map(this::decodeCompletedOperation)
            .collect(Collectors.toList());

    return new ClusterChangePlan(
        clusterChangePlan.getId(),
        clusterChangePlan.getVersion(),
        toChangeStatus(clusterChangePlan.getStatus()),
        Instant.ofEpochSecond(
            clusterChangePlan.getStartedAt().getSeconds(),
            clusterChangePlan.getStartedAt().getNanos()),
        completedOperations,
        pendingOperations);
  }

  private io.camunda.zeebe.topology.state.CompletedChange decodeCompletedChange(
      final CompletedChange completedChange) {
    return new io.camunda.zeebe.topology.state.CompletedChange(
        completedChange.getId(),
        toChangeStatus(completedChange.getStatus()),
        Instant.ofEpochSecond(
            completedChange.getStartedAt().getSeconds(), completedChange.getStartedAt().getNanos()),
        Instant.ofEpochSecond(
            completedChange.getCompletedAt().getSeconds(),
            completedChange.getCompletedAt().getNanos()));
  }

  private TopologyChangeOperation decodeOperation(
      final Topology.TopologyChangeOperation topologyChangeOperation) {
    if (topologyChangeOperation.hasPartitionJoin()) {
      return new PartitionJoinOperation(
          MemberId.from(topologyChangeOperation.getMemberId()),
          topologyChangeOperation.getPartitionJoin().getPartitionId(),
          topologyChangeOperation.getPartitionJoin().getPriority());
    } else if (topologyChangeOperation.hasPartitionLeave()) {
      return new PartitionLeaveOperation(
          MemberId.from(topologyChangeOperation.getMemberId()),
          topologyChangeOperation.getPartitionLeave().getPartitionId());
    } else if (topologyChangeOperation.hasMemberJoin()) {
      return new MemberJoinOperation(MemberId.from(topologyChangeOperation.getMemberId()));
    } else if (topologyChangeOperation.hasMemberLeave()) {
      return new MemberLeaveOperation(MemberId.from(topologyChangeOperation.getMemberId()));
    } else if (topologyChangeOperation.hasPartitionReconfigurePriority()) {
      return new PartitionReconfigurePriorityOperation(
          MemberId.from(topologyChangeOperation.getMemberId()),
          topologyChangeOperation.getPartitionReconfigurePriority().getPartitionId(),
          topologyChangeOperation.getPartitionReconfigurePriority().getPriority());
    } else if (topologyChangeOperation.hasPartitionForceReconfigure()) {
      return new PartitionForceReconfigureOperation(
          MemberId.from(topologyChangeOperation.getMemberId()),
          topologyChangeOperation.getPartitionForceReconfigure().getPartitionId(),
          topologyChangeOperation.getPartitionForceReconfigure().getMembersList().stream()
              .map(MemberId::from)
              .toList());
    } else if (topologyChangeOperation.hasMemberRemove()) {
      return new MemberRemoveOperation(
          MemberId.from(topologyChangeOperation.getMemberId()),
          MemberId.from(topologyChangeOperation.getMemberRemove().getMemberToRemove()));
    } else if (topologyChangeOperation.hasPartitionExporterDisable()) {
      return new PartitionDisableExporterOperation(
          MemberId.from(topologyChangeOperation.getMemberId()),
          topologyChangeOperation.getPartitionExporterDisable().getPartitionId(),
          topologyChangeOperation.getPartitionExporterDisable().getExporterId());
    } else if (topologyChangeOperation.hasPartitionExporterEnable()) {
      return new PartitionEnableExporterOperation(
          MemberId.from(topologyChangeOperation.getMemberId()),
          topologyChangeOperation.getPartitionExporterEnable().getPartitionId(),
          topologyChangeOperation.getPartitionExporterEnable().getExporterId());
    } else {
      // If the node does not know of a type, the exception thrown will prevent
      // ClusterTopologyGossiper from processing the incoming topology. This helps to prevent any
      // incorrect or partial topology to be stored locally and later propagated to other nodes.
      // Ideally, it is better not to any cluster topology change operations execute during a
      // rolling update.
      throw new IllegalStateException("Unknown operation: " + topologyChangeOperation);
    }
  }

  private CompletedOperation decodeCompletedOperation(
      final CompletedTopologyChangeOperation operation) {
    return new CompletedOperation(
        decodeOperation(operation.getOperation()),
        Instant.ofEpochSecond(operation.getCompletedAt().getSeconds()));
  }

  @Override
  public byte[] encodeAddMembersRequest(final AddMembersRequest req) {
    return Requests.AddMembersRequest.newBuilder()
        .addAllMemberIds(req.members().stream().map(MemberId::id).toList())
        .setDryRun(req.dryRun())
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeRemoveMembersRequest(final RemoveMembersRequest req) {
    return Requests.RemoveMembersRequest.newBuilder()
        .addAllMemberIds(req.members().stream().map(MemberId::id).toList())
        .setDryRun(req.dryRun())
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeJoinPartitionRequest(final JoinPartitionRequest req) {
    return Requests.JoinPartitionRequest.newBuilder()
        .setMemberId(req.memberId().id())
        .setPartitionId(req.partitionId())
        .setPriority(req.priority())
        .setDryRun(req.dryRun())
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeLeavePartitionRequest(final LeavePartitionRequest req) {
    return Requests.LeavePartitionRequest.newBuilder()
        .setMemberId(req.memberId().id())
        .setPartitionId(req.partitionId())
        .setDryRun(req.dryRun())
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeReassignPartitionsRequest(
      final ReassignPartitionsRequest reassignPartitionsRequest) {
    return ReassignAllPartitionsRequest.newBuilder()
        .addAllMemberIds(reassignPartitionsRequest.members().stream().map(MemberId::id).toList())
        .setDryRun(reassignPartitionsRequest.dryRun())
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeScaleRequest(final ScaleRequest scaleRequest) {
    final var builder =
        Requests.ScaleRequest.newBuilder()
            .addAllMemberIds(scaleRequest.members().stream().map(MemberId::id).toList())
            .setDryRun(scaleRequest.dryRun());

    scaleRequest.newReplicationFactor().ifPresent(builder::setNewReplicationFactor);

    return builder.build().toByteArray();
  }

  @Override
  public byte[] encodeCancelChangeRequest(final CancelChangeRequest cancelChangeRequest) {
    return CancelTopologyChangeRequest.newBuilder()
        .setChangeId(cancelChangeRequest.changeId())
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeExporterEnableRequest(final EnableExporterRequest req) {
    return Requests.ExporterEnableRequest.newBuilder()
        .setExporterId(req.exporterId())
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeExporterDisableRequest(final DisableExporterRequest req) {
    return Requests.ExporterDisableRequest.newBuilder()
        .setExporterId(req.exporterId())
        .build()
        .toByteArray();
  }

  @Override
  public AddMembersRequest decodeAddMembersRequest(final byte[] encodedState) {
    try {
      final var addMemberRequest = Requests.AddMembersRequest.parseFrom(encodedState);
      return new AddMembersRequest(
          addMemberRequest.getMemberIdsList().stream()
              .map(MemberId::from)
              .collect(Collectors.toSet()),
          addMemberRequest.getDryRun());
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public RemoveMembersRequest decodeRemoveMembersRequest(final byte[] encodedState) {
    try {
      final var removeMemberRequest = Requests.RemoveMembersRequest.parseFrom(encodedState);
      return new RemoveMembersRequest(
          removeMemberRequest.getMemberIdsList().stream()
              .map(MemberId::from)
              .collect(Collectors.toSet()),
          removeMemberRequest.getDryRun());
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public JoinPartitionRequest decodeJoinPartitionRequest(final byte[] encodedState) {
    try {
      final var joinPartitionRequest = Requests.JoinPartitionRequest.parseFrom(encodedState);
      return new JoinPartitionRequest(
          MemberId.from(joinPartitionRequest.getMemberId()),
          joinPartitionRequest.getPartitionId(),
          joinPartitionRequest.getPriority(),
          joinPartitionRequest.getDryRun());
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public LeavePartitionRequest decodeLeavePartitionRequest(final byte[] encodedState) {
    try {
      final var leavePartitionRequest = Requests.LeavePartitionRequest.parseFrom(encodedState);
      return new LeavePartitionRequest(
          MemberId.from(leavePartitionRequest.getMemberId()),
          leavePartitionRequest.getPartitionId(),
          leavePartitionRequest.getDryRun());
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public ReassignPartitionsRequest decodeReassignPartitionsRequest(final byte[] encodedState) {
    try {
      final var reassignPartitionsRequest = ReassignAllPartitionsRequest.parseFrom(encodedState);
      return new ReassignPartitionsRequest(
          reassignPartitionsRequest.getMemberIdsList().stream()
              .map(MemberId::from)
              .collect(Collectors.toSet()),
          reassignPartitionsRequest.getDryRun());
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public ScaleRequest decodeScaleRequest(final byte[] encodedState) {
    try {
      final var scaleRequest = Requests.ScaleRequest.parseFrom(encodedState);
      final Optional<Integer> newReplicationFactor =
          scaleRequest.hasNewReplicationFactor()
              ? Optional.of(scaleRequest.getNewReplicationFactor())
              : Optional.empty();
      return new ScaleRequest(
          scaleRequest.getMemberIdsList().stream().map(MemberId::from).collect(Collectors.toSet()),
          newReplicationFactor,
          scaleRequest.getDryRun());
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public CancelChangeRequest decodeCancelChangeRequest(final byte[] encodedState) {
    try {
      final var cancelChangeRequest = CancelTopologyChangeRequest.parseFrom(encodedState);
      return new CancelChangeRequest(cancelChangeRequest.getChangeId());
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public EnableExporterRequest decodeExporterEnableRequest(final byte[] encodedState) {
    try {
      final var enableExporterRequest = Requests.ExporterEnableRequest.parseFrom(encodedState);
      return new EnableExporterRequest(enableExporterRequest.getExporterId(), false);
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public DisableExporterRequest decodeExporterDisableRequest(final byte[] encodedState) {
    try {
      final var disableExporterRequest = Requests.ExporterDisableRequest.parseFrom(encodedState);
      return new DisableExporterRequest(disableExporterRequest.getExporterId(), false);
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public byte[] encodeResponse(final TopologyChangeResponse response) {
    return Response.newBuilder()
        .setTopologyChangeResponse(encodeTopologyChangeResponse(response))
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeResponse(final ClusterTopology response) {
    return Response.newBuilder()
        .setClusterTopology(encodeClusterTopology(response))
        .build()
        .toByteArray();
  }

  @Override
  public byte[] encodeResponse(final ErrorResponse response) {
    return Response.newBuilder()
        .setError(
            Requests.ErrorResponse.newBuilder()
                .setErrorCode(encodeErrorCode(response.code()))
                .setErrorMessage(response.message()))
        .build()
        .toByteArray();
  }

  @Override
  public Either<ErrorResponse, TopologyChangeResponse> decodeTopologyChangeResponse(
      final byte[] encodedResponse) {
    try {
      final var response = Response.parseFrom(encodedResponse);
      if (response.hasError()) {
        return Either.left(
            new ErrorResponse(
                decodeErrorCode(response.getError().getErrorCode()),
                response.getError().getErrorMessage()));
      } else if (response.hasTopologyChangeResponse()) {
        return Either.right(decodeTopologyChangeResponse(response.getTopologyChangeResponse()));
      } else {
        throw new DecodingFailed(
            "Response does not have an error or a valid topology change response");
      }

    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  @Override
  public Either<ErrorResponse, ClusterTopology> decodeClusterTopologyResponse(
      final byte[] encodedResponse) {
    try {
      final var response = Response.parseFrom(encodedResponse);
      if (response.hasError()) {
        return Either.left(
            new ErrorResponse(
                decodeErrorCode(response.getError().getErrorCode()),
                response.getError().getErrorMessage()));
      } else if (response.hasClusterTopology()) {
        return Either.right(decodeClusterTopology(response.getClusterTopology()));
      } else {
        throw new DecodingFailed("Response does not have an error or a valid cluster topology");
      }
    } catch (final InvalidProtocolBufferException e) {
      throw new DecodingFailed(e);
    }
  }

  public Builder encodeTopologyChangeResponse(final TopologyChangeResponse topologyChangeResponse) {
    final var builder = Requests.TopologyChangeResponse.newBuilder();

    builder
        .setChangeId(topologyChangeResponse.changeId())
        .addAllPlannedChanges(
            topologyChangeResponse.plannedChanges().stream().map(this::encodeOperation).toList())
        .putAllCurrentTopology(encodeMemberStateMap(topologyChangeResponse.currentTopology()))
        .putAllExpectedTopology(encodeMemberStateMap(topologyChangeResponse.expectedTopology()));

    return builder;
  }

  public TopologyChangeResponse decodeTopologyChangeResponse(
      final Requests.TopologyChangeResponse topologyChangeResponse) {
    return new TopologyChangeResponse(
        topologyChangeResponse.getChangeId(),
        decodeMemberStateMap(topologyChangeResponse.getCurrentTopologyMap()),
        decodeMemberStateMap(topologyChangeResponse.getExpectedTopologyMap()),
        topologyChangeResponse.getPlannedChangesList().stream()
            .map(this::decodeOperation)
            .collect(Collectors.toList()));
  }

  private ErrorCode encodeErrorCode(final ErrorResponse.ErrorCode status) {
    return switch (status) {
      case INVALID_REQUEST -> ErrorCode.INVALID_REQUEST;
      case OPERATION_NOT_ALLOWED -> ErrorCode.OPERATION_NOT_ALLOWED;
      case CONCURRENT_MODIFICATION -> ErrorCode.CONCURRENT_MODIFICATION;
      case INTERNAL_ERROR -> ErrorCode.INTERNAL_ERROR;
    };
  }

  private ErrorResponse.ErrorCode decodeErrorCode(final ErrorCode status) {
    return switch (status) {
      case INVALID_REQUEST -> ErrorResponse.ErrorCode.INVALID_REQUEST;
      case OPERATION_NOT_ALLOWED -> ErrorResponse.ErrorCode.OPERATION_NOT_ALLOWED;
      case CONCURRENT_MODIFICATION -> ErrorResponse.ErrorCode.CONCURRENT_MODIFICATION;
      case INTERNAL_ERROR, UNRECOGNIZED -> ErrorResponse.ErrorCode.INTERNAL_ERROR;
    };
  }

  private Map<String, MemberState> encodeMemberStateMap(
      final Map<MemberId, io.camunda.zeebe.topology.state.MemberState> topologyChangeResponse) {
    return topologyChangeResponse.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().id(), e -> encodeMemberState(e.getValue())));
  }

  private ChangeStatus fromTopologyChangeStatus(final Status status) {
    return switch (status) {
      case IN_PROGRESS -> ChangeStatus.IN_PROGRESS;
      case COMPLETED -> ChangeStatus.COMPLETED;
      case FAILED -> ChangeStatus.FAILED;
      case CANCELLED -> ChangeStatus.CANCELLED;
    };
  }

  private Status toChangeStatus(final ChangeStatus status) {
    return switch (status) {
      case IN_PROGRESS -> Status.IN_PROGRESS;
      case COMPLETED -> Status.COMPLETED;
      case FAILED -> Status.FAILED;
      case CANCELLED -> Status.CANCELLED;
      default -> throw new IllegalStateException("Unknown status: " + status);
    };
  }

  private Topology.DynamicConfiguration encodeDynamicConfiguration(
      final DynamicConfiguration configuration) {
    final var builder = Topology.DynamicConfiguration.newBuilder();
    configuration
        .exporters()
        .forEach(
            (expoterId, config) -> builder.putExporters(expoterId, encodeExporterConfig(config)));
    return builder.build();
  }

  private DynamicConfiguration decodeDynamicConfig(
      final Topology.DynamicConfiguration configuration) {
    final var exporters =
        configuration.getExportersMap().entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> decodeExporterConfig(e.getValue())));
    return new DynamicConfiguration(exporters);
  }

  private DynamicConfiguration.ExporterConfig decodeExporterConfig(
      final ExporterConfig encodedConfig) {
    return new DynamicConfiguration.ExporterConfig(
        decodeExporterState(encodedConfig.getState()), Optional.empty());
  }

  private ExporterConfig encodeExporterConfig(final DynamicConfiguration.ExporterConfig config) {
    return ExporterConfig.newBuilder().setState(encodeExporterState(config.state())).build();
  }

  private ExporterState encodeExporterState(
      final DynamicConfiguration.ExporterConfig.ExporterState state) {
    return switch (state) {
      case ENABLED -> ExporterState.ENABLED;
      case DISABLED -> ExporterState.DISABLED;
    };
  }

  private DynamicConfiguration.ExporterConfig.ExporterState decodeExporterState(
      final ExporterState state) {
    return switch (state) {
      case ENABLED -> DynamicConfiguration.ExporterConfig.ExporterState.ENABLED;
      case DISABLED -> DynamicConfiguration.ExporterConfig.ExporterState.DISABLED;
      default -> throw new IllegalStateException("Unknown status: " + state);
    };
  }
}
