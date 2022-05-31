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
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class RemoteCommandReceiver {

  private LogStreamRecordWriter logStreamRecordWriter;

  public void registerCommandHandler(
      final String commandType, final Function<byte[], TransformedRequest> requestTransformer) {}

  private void handleRequest(final String commandType, final byte[] request) {

    final DirectBuffer buffer = new UnsafeBuffer(request);

    final InterPartitionCommandMetaDataDecoder metaDataDecoder =
        new InterPartitionCommandMetaDataDecoder();
    metaDataDecoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
    final var checkpointId = metaDataDecoder.checkpointId();
    final var partitonId = metaDataDecoder.partitionId();

    final RemoteCommandHandler messageHandler = getHandler(commandType);
    final TransformedRequest commandToWrite =
        messageHandler.apply(buffer, metaDataDecoder.encodedLength());

    logStreamRecordWriter.reset();
    final RecordMetadata recordMetadata = new RecordMetadata();
    recordMetadata
        .reset()
        .recordType(RecordType.COMMAND)
        .valueType(commandToWrite.valueType())
        .intent(commandToWrite.intent());
    // .checkpointId(checkpointId);

    logStreamRecordWriter
        .key(-1)
        .metadataWriter(recordMetadata)
        .valueWriter(commandToWrite.record())
        .tryWrite();
  }

  private RemoteCommandHandler getHandler(final String commandType) {
    return null;
  }

  public record TransformedRequest(ValueType valueType, Intent intent, UnpackedObject record) {}
}
