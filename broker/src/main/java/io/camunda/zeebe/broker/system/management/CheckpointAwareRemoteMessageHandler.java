/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system.management;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.camunda.zeebe.broker.Loggers;
import io.camunda.zeebe.broker.PartitionListener;
import io.camunda.zeebe.broker.remote.RemoteCommandHandler;
import io.camunda.zeebe.broker.system.management.deployment.PushDeploymentRemoteCommandHandler;
import io.camunda.zeebe.broker.system.monitoring.DiskSpaceUsageListener;
import io.camunda.zeebe.clustering.management.InterPartitionCommandMetaDataDecoder;
import io.camunda.zeebe.clustering.management.MessageHeaderDecoder;
import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessor;
import io.camunda.zeebe.engine.state.QueryService;
import io.camunda.zeebe.logstreams.log.LogStream;
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter;
import io.camunda.zeebe.protocol.impl.encoding.BrokerInfo;
import io.camunda.zeebe.protocol.impl.encoding.ErrorResponse;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.value.checkpoint.CheckpointRecord;
import io.camunda.zeebe.protocol.record.ErrorCode;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.CheckpointIntent;
import io.camunda.zeebe.util.buffer.BufferUtil;
import io.camunda.zeebe.util.sched.Actor;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import io.camunda.zeebe.util.sched.future.CompletableActorFuture;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

public final class CheckpointAwareRemoteMessageHandler extends Actor
    implements PartitionListener, DiskSpaceUsageListener {
  private static final Logger LOG = Loggers.CLUSTERING_LOGGER;
  private static final String DEPLOYMENT_TOPIC = "deployment";
  private static final String SUBSCRIPTION_TOPIC = "subscription";
  private final Int2ObjectHashMap<LogStreamRecordWriter> leaderForPartitions =
      new Int2ObjectHashMap<>();

  private final Int2ObjectHashMap<Long> partitionCheckpointIds = new Int2ObjectHashMap<>();
  private final Int2ObjectHashMap<StreamProcessor> partitionStreamProcessors =
      new Int2ObjectHashMap<>();

  private final String actorName;
  private final ErrorResponse outOfDiskSpaceError;
  private final ClusterCommunicationService communicationService;

  private final Map<String, RemoteCommandHandler> remoteCommandHandlerMap = new HashMap<>();

  public CheckpointAwareRemoteMessageHandler(
      final BrokerInfo localBroker,
      final ClusterCommunicationService communicationService,
      final ClusterEventService eventService) {
    this.communicationService = communicationService;
    actorName = buildActorName(localBroker.getNodeId(), "ManagementRequestHandler");
    outOfDiskSpaceError = new ErrorResponse();
    outOfDiskSpaceError
        .setErrorCode(ErrorCode.RESOURCE_EXHAUSTED)
        .setErrorData(
            BufferUtil.wrapString(
                String.format(
                    "Broker %d is out of disk space. Rejecting deployment request.",
                    localBroker.getNodeId())));

    remoteCommandHandlerMap.put("deployment", new PushDeploymentRemoteCommandHandler());
    remoteCommandHandlerMap.put("subscription", new SubscriptionRemoteCommandHandler());
  }

  private void handleRequest(
      final String commandType, final byte[] request, final CompletableFuture<Void> future) {

    final DirectBuffer buffer = new UnsafeBuffer(request);

    final InterPartitionCommandMetaDataDecoder metaDataDecoder =
        new InterPartitionCommandMetaDataDecoder();
    metaDataDecoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
    final var checkpointId = metaDataDecoder.checkpointId();
    final var partitionId = metaDataDecoder.partitionId();

    partitionStreamProcessors
        .get(partitionId)
        .getCheckpointIdAsync()
        .onComplete(
            (id, error) -> {
              partitionCheckpointIds.put(partitionId, id);
            });

    final long currentCheckpointId = partitionCheckpointIds.getOrDefault(partitionId, 0L);
    final var logStreamRecordWriter = leaderForPartitions.get(partitionId);

    if (logStreamRecordWriter == null) {
      future.completeExceptionally(new RuntimeException("Not leader"));
    }

    // First write the checkpoint command. No need to use batch writer because, it is ok if only
    // checkpoint command is written. No need to be atomic.
    if (currentCheckpointId < checkpointId) {
      logStreamRecordWriter.reset();
      final RecordMetadata recordMetadata = new RecordMetadata();

      recordMetadata
          .reset()
          .recordType(RecordType.COMMAND)
          .valueType(ValueType.CHECKPOINT)
          .intent(CheckpointIntent.CREATE);
      logStreamRecordWriter
          .key(-1)
          .metadataWriter(recordMetadata)
          .valueWriter(new CheckpointRecord().setCheckpointId(checkpointId))
          .tryWrite();
    }
    // then write the received command

    final RemoteCommandHandler messageHandler = getHandler(commandType);
    try {
      messageHandler.apply(
          buffer,
          metaDataDecoder.encodedLength(),
          buffer.capacity() - metaDataDecoder.encodedLength(),
          logStreamRecordWriter);
      future.complete(null);
    } catch (final Exception e) {
      future.completeExceptionally(e);
    }
  }

  private RemoteCommandHandler getHandler(final String commandType) {
    return remoteCommandHandlerMap.get(commandType);
  }

  @Override
  public ActorFuture<Void> onBecomingFollower(final int partitionId, final long term) {
    return actor.call(
        () -> {
          leaderForPartitions.remove(partitionId);
          partitionStreamProcessors.remove(partitionId);
          return null;
        });
  }

  @Override
  public ActorFuture<Void> onBecomingLeader(
      final int partitionId,
      final long term,
      final LogStream logStream,
      final QueryService queryService) {
    return null;
  }

  @Override
  public ActorFuture<Void> onBecomingLeader(
      final int partitionId,
      final long term,
      final LogStream logStream,
      final QueryService queryService,
      final StreamProcessor streamProcessor) {
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    actor.submit(
        () ->
            logStream
                .newLogStreamRecordWriter()
                .onComplete(
                    (recordWriter, error) -> {
                      if (error == null) {
                        leaderForPartitions.put(partitionId, recordWriter);
                        partitionStreamProcessors.put(partitionId, streamProcessor);
                        future.complete(null);
                      } else {
                        LOG.error(
                            "Unexpected error on retrieving write buffer for partition {}",
                            partitionId,
                            error);
                        future.completeExceptionally(error);
                      }
                    }));
    return future;
  }

  @Override
  public ActorFuture<Void> onBecomingInactive(final int partitionId, final long term) {
    return actor.call(
        () -> {
          leaderForPartitions.remove(partitionId);
          return null;
        });
  }

  @Override
  public String getName() {
    return actorName;
  }

  @Override
  protected void onActorStarting() {

    communicationService.subscribe(DEPLOYMENT_TOPIC, this::handleDeploymentCommand);
    communicationService.subscribe(SUBSCRIPTION_TOPIC, this::handleSubscriptionCommand);
  }

  private CompletableFuture<Void> handleSubscriptionCommand(final byte[] request) {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    handleRequest(SUBSCRIPTION_TOPIC, request, future);
    return future;
  }

  private CompletableFuture<Void> handleDeploymentCommand(final byte[] request) {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    handleRequest(DEPLOYMENT_TOPIC, request, future);
    return future;
  }

  @Override
  public void onDiskSpaceNotAvailable() {
    actor.call(
        () -> {
          LOG.debug(
              "Broker is out of disk space. All requests with topic {} will be rejected.",
              DEPLOYMENT_TOPIC);
          communicationService.unsubscribe(DEPLOYMENT_TOPIC);
          communicationService.subscribe(
              DEPLOYMENT_TOPIC,
              b -> CompletableFuture.completedFuture(outOfDiskSpaceError.toBytes()));
          communicationService.unsubscribe(SUBSCRIPTION_TOPIC);
          communicationService.subscribe(
              SUBSCRIPTION_TOPIC,
              b -> CompletableFuture.completedFuture(outOfDiskSpaceError.toBytes()));
        });
  }

  @Override
  public void onDiskSpaceAvailable() {
    actor.call(
        () -> {
          LOG.debug(
              "Broker has disk space available again. All requests with topic {} will be accepted.",
              DEPLOYMENT_TOPIC);
          communicationService.unsubscribe(DEPLOYMENT_TOPIC);
          communicationService.unsubscribe(SUBSCRIPTION_TOPIC);
          communicationService.subscribe(DEPLOYMENT_TOPIC, this::handleDeploymentCommand);
          communicationService.subscribe(SUBSCRIPTION_TOPIC, this::handleSubscriptionCommand);
        });
  }
}
