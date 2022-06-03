/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system.management.deployment;

import io.camunda.zeebe.broker.Loggers;
import io.camunda.zeebe.broker.remote.RemoteCommandHandler;
import io.camunda.zeebe.clustering.management.MessageHeaderDecoder;
import io.camunda.zeebe.clustering.management.PushDeploymentRequestDecoder;
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.camunda.zeebe.protocol.record.intent.Intent;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public final class PushDeploymentRemoteCommandHandler implements RemoteCommandHandler {

  private static final Logger LOG = Loggers.PROCESS_REPOSITORY_LOGGER;

  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

  private final RecordMetadata recordMetadata = new RecordMetadata();

  private void handleValidRequest(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamWriter) {
    final PushDeploymentRequest pushDeploymentRequest = new PushDeploymentRequest();
    pushDeploymentRequest.wrap(buffer, offset, length);
    final long deploymentKey = pushDeploymentRequest.deploymentKey();
    final DirectBuffer deployment = pushDeploymentRequest.deployment();

    handlePushDeploymentRequest(deployment, deploymentKey, logStreamWriter);
  }

  private void handlePushDeploymentRequest(
      final DirectBuffer deployment,
      final long deploymentKey,
      final LogStreamRecordWriter logStreamWriter) {

    final DeploymentRecord deploymentRecord = new DeploymentRecord();
    deploymentRecord.wrap(deployment);

    final boolean success =
        writeDistributeDeployment(logStreamWriter, deploymentKey, deploymentRecord);
    if (!success) {
      throw new RuntimeException("Failed to write the command to logstream");
    }
  }

  private boolean writeDistributeDeployment(
      final LogStreamRecordWriter logStreamWriter, final long key, final UnpackedObject event) {
    final RecordType recordType = RecordType.COMMAND;
    final ValueType valueType = ValueType.DEPLOYMENT;
    final Intent intent = DeploymentIntent.DISTRIBUTE;

    logStreamWriter.reset();
    recordMetadata.reset().recordType(recordType).valueType(valueType).intent(intent);

    final long position =
        logStreamWriter.key(key).metadataWriter(recordMetadata).valueWriter(event).tryWrite();

    return position > 0;
  }

  @Override
  public void apply(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamWriter) {
    messageHeaderDecoder.wrap(buffer, offset);
    final int schemaId = messageHeaderDecoder.schemaId();

    if (PushDeploymentRequestDecoder.SCHEMA_ID == schemaId) {
      final int templateId = messageHeaderDecoder.templateId();
      if (PushDeploymentRequestDecoder.TEMPLATE_ID == templateId) {
        handleValidRequest(buffer, offset, length, logStreamWriter);
      } else {
        final String errorMsg =
            String.format(
                "Expected to have template id %d, but got %d.",
                PushDeploymentRequestDecoder.TEMPLATE_ID, templateId);
        throw new RuntimeException(errorMsg);
      }
    } else {
      final String errorMsg =
          String.format(
              "Expected to have schema id %d, but got %d.",
              PushDeploymentRequestDecoder.SCHEMA_ID, schemaId);
      throw new RuntimeException(errorMsg);
    }
  }
}
