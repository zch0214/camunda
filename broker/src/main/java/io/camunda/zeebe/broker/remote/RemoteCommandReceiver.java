/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.remote;

import io.camunda.zeebe.clustering.management.InterPartitionCommandMetaDataDecoder;
import io.camunda.zeebe.clustering.management.MessageHeaderDecoder;
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.value.checkpoint.CheckpointRecord;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.CheckpointIntent;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

// This should be in the same package as StreamProcessor. As broker shouldn't care about
// checkpointIds.
public class RemoteCommandReceiver {

  private long currentCheckpointId; // Should get it from StreamProcessor. May be add a listener
  private final LogStreamRecordWriter logStreamRecordWriter;
  private final Supplier<ActorFuture<Long>> checkpointIdSupplier;

  public RemoteCommandReceiver(
      final LogStreamRecordWriter logStreamRecordWriter,
      final Supplier<ActorFuture<Long>> checkpointIdSupplier) {
    this.logStreamRecordWriter = logStreamRecordWriter;
    this.checkpointIdSupplier = checkpointIdSupplier;
  }

  // This must be used by PushDeploymentRequestHandler and SubscriptionMessageHandler
  public void registerCommandHandler(
      final String commandType, final Consumer<byte[]> requestHandler) {}

  private void handleRequest(final String commandType, final byte[] request) {

    checkpointIdSupplier
        .get()
        .onComplete(
            (id, error) -> {
              currentCheckpointId = id;
            });

    final DirectBuffer buffer = new UnsafeBuffer(request);

    final InterPartitionCommandMetaDataDecoder metaDataDecoder =
        new InterPartitionCommandMetaDataDecoder();
    metaDataDecoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
    final var checkpointId = metaDataDecoder.checkpointId();
    final var partitionId = metaDataDecoder.partitionId();

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
    messageHandler.apply(
        buffer,
        metaDataDecoder.encodedLength(),
        buffer.capacity() - metaDataDecoder.encodedLength(),
        logStreamRecordWriter); // TODO: handle buffer correctly
  }

  private RemoteCommandHandler getHandler(final String commandType) {
    return null;
  }
}
