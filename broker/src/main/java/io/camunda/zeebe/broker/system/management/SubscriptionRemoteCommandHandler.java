/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system.management;

import io.camunda.zeebe.broker.remote.RemoteCommandHandler;
import io.camunda.zeebe.engine.processing.message.command.CloseMessageSubscriptionCommand;
import io.camunda.zeebe.engine.processing.message.command.CloseMessageSubscriptionDecoder;
import io.camunda.zeebe.engine.processing.message.command.CloseProcessMessageSubscriptionCommand;
import io.camunda.zeebe.engine.processing.message.command.CloseProcessMessageSubscriptionDecoder;
import io.camunda.zeebe.engine.processing.message.command.CorrelateMessageSubscriptionCommand;
import io.camunda.zeebe.engine.processing.message.command.CorrelateMessageSubscriptionDecoder;
import io.camunda.zeebe.engine.processing.message.command.CorrelateProcessMessageSubscriptionCommand;
import io.camunda.zeebe.engine.processing.message.command.CorrelateProcessMessageSubscriptionDecoder;
import io.camunda.zeebe.engine.processing.message.command.MessageHeaderDecoder;
import io.camunda.zeebe.engine.processing.message.command.OpenMessageSubscriptionCommand;
import io.camunda.zeebe.engine.processing.message.command.OpenMessageSubscriptionDecoder;
import io.camunda.zeebe.engine.processing.message.command.OpenProcessMessageSubscriptionCommand;
import io.camunda.zeebe.engine.processing.message.command.OpenProcessMessageSubscriptionDecoder;
import io.camunda.zeebe.engine.processing.message.command.RejectCorrelateMessageSubscriptionCommand;
import io.camunda.zeebe.engine.processing.message.command.RejectCorrelateMessageSubscriptionDecoder;
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.Protocol;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.value.message.MessageSubscriptionRecord;
import io.camunda.zeebe.protocol.impl.record.value.message.ProcessMessageSubscriptionRecord;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessMessageSubscriptionIntent;
import org.agrona.DirectBuffer;

public final class SubscriptionRemoteCommandHandler implements RemoteCommandHandler {

  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

  private final OpenMessageSubscriptionCommand openMessageSubscriptionCommand =
      new OpenMessageSubscriptionCommand();

  private final OpenProcessMessageSubscriptionCommand openProcessMessageSubscriptionCommand =
      new OpenProcessMessageSubscriptionCommand();

  private final CorrelateProcessMessageSubscriptionCommand
      correlateProcessMessageSubscriptionCommand = new CorrelateProcessMessageSubscriptionCommand();

  private final CorrelateMessageSubscriptionCommand correlateMessageSubscriptionCommand =
      new CorrelateMessageSubscriptionCommand();

  private final CloseMessageSubscriptionCommand closeMessageSubscriptionCommand =
      new CloseMessageSubscriptionCommand();

  private final CloseProcessMessageSubscriptionCommand closeProcessMessageSubscriptionCommand =
      new CloseProcessMessageSubscriptionCommand();

  private final RejectCorrelateMessageSubscriptionCommand resetMessageCorrelationCommand =
      new RejectCorrelateMessageSubscriptionCommand();

  private final RecordMetadata recordMetadata = new RecordMetadata();

  private final MessageSubscriptionRecord messageSubscriptionRecord =
      new MessageSubscriptionRecord();

  private final ProcessMessageSubscriptionRecord processMessageSubscriptionRecord =
      new ProcessMessageSubscriptionRecord();

  private boolean onOpenMessageSubscription(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    openMessageSubscriptionCommand.wrap(buffer, offset, length);

    messageSubscriptionRecord
        .setProcessInstanceKey(openMessageSubscriptionCommand.getProcessInstanceKey())
        .setElementInstanceKey(openMessageSubscriptionCommand.getElementInstanceKey())
        .setBpmnProcessId(openMessageSubscriptionCommand.getBpmnProcessId())
        .setMessageKey(-1)
        .setMessageName(openMessageSubscriptionCommand.getMessageName())
        .setCorrelationKey(openMessageSubscriptionCommand.getCorrelationKey())
        .setInterrupting(openMessageSubscriptionCommand.shouldCloseOnCorrelate());

    return writeCommand(
        openMessageSubscriptionCommand.getSubscriptionPartitionId(),
        ValueType.MESSAGE_SUBSCRIPTION,
        MessageSubscriptionIntent.CREATE,
        messageSubscriptionRecord,
        logStreamRecordWriter);
  }

  private boolean onOpenProcessMessageSubscription(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    openProcessMessageSubscriptionCommand.wrap(buffer, offset, length);

    final long processInstanceKey = openProcessMessageSubscriptionCommand.getProcessInstanceKey();
    final int processInstancePartitionId = Protocol.decodePartitionId(processInstanceKey);

    processMessageSubscriptionRecord.reset();
    processMessageSubscriptionRecord
        .setSubscriptionPartitionId(
            openProcessMessageSubscriptionCommand.getSubscriptionPartitionId())
        .setProcessInstanceKey(processInstanceKey)
        .setElementInstanceKey(openProcessMessageSubscriptionCommand.getElementInstanceKey())
        .setMessageKey(-1)
        .setMessageName(openProcessMessageSubscriptionCommand.getMessageName())
        .setInterrupting(openProcessMessageSubscriptionCommand.shouldCloseOnCorrelate());

    return writeCommand(
        processInstancePartitionId,
        ValueType.PROCESS_MESSAGE_SUBSCRIPTION,
        ProcessMessageSubscriptionIntent.CREATE,
        processMessageSubscriptionRecord,
        logStreamRecordWriter);
  }

  private boolean onCorrelateProcessMessageSubscription(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    correlateProcessMessageSubscriptionCommand.wrap(buffer, offset, length);

    final long processInstanceKey =
        correlateProcessMessageSubscriptionCommand.getProcessInstanceKey();
    final int processInstancePartitionId = Protocol.decodePartitionId(processInstanceKey);

    processMessageSubscriptionRecord
        .setSubscriptionPartitionId(
            correlateProcessMessageSubscriptionCommand.getSubscriptionPartitionId())
        .setProcessInstanceKey(processInstanceKey)
        .setElementInstanceKey(correlateProcessMessageSubscriptionCommand.getElementInstanceKey())
        .setBpmnProcessId(correlateProcessMessageSubscriptionCommand.getBpmnProcessId())
        .setMessageKey(correlateProcessMessageSubscriptionCommand.getMessageKey())
        .setMessageName(correlateProcessMessageSubscriptionCommand.getMessageName())
        .setVariables(correlateProcessMessageSubscriptionCommand.getVariables())
        .setCorrelationKey(correlateProcessMessageSubscriptionCommand.getCorrelationKey());

    return writeCommand(
        processInstancePartitionId,
        ValueType.PROCESS_MESSAGE_SUBSCRIPTION,
        ProcessMessageSubscriptionIntent.CORRELATE,
        processMessageSubscriptionRecord,
        logStreamRecordWriter);
  }

  private boolean onCorrelateMessageSubscription(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    correlateMessageSubscriptionCommand.wrap(buffer, offset, length);

    messageSubscriptionRecord.reset();
    messageSubscriptionRecord
        .setProcessInstanceKey(correlateMessageSubscriptionCommand.getProcessInstanceKey())
        .setElementInstanceKey(correlateMessageSubscriptionCommand.getElementInstanceKey())
        .setBpmnProcessId(correlateMessageSubscriptionCommand.getBpmnProcessId())
        .setMessageKey(-1)
        .setMessageName(correlateMessageSubscriptionCommand.getMessageName());

    return writeCommand(
        correlateMessageSubscriptionCommand.getSubscriptionPartitionId(),
        ValueType.MESSAGE_SUBSCRIPTION,
        MessageSubscriptionIntent.CORRELATE,
        messageSubscriptionRecord,
        logStreamRecordWriter);
  }

  private boolean onCloseMessageSubscription(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    closeMessageSubscriptionCommand.wrap(buffer, offset, length);

    messageSubscriptionRecord.reset();
    messageSubscriptionRecord
        .setProcessInstanceKey(closeMessageSubscriptionCommand.getProcessInstanceKey())
        .setElementInstanceKey(closeMessageSubscriptionCommand.getElementInstanceKey())
        .setMessageKey(-1L)
        .setMessageName(closeMessageSubscriptionCommand.getMessageName());

    return writeCommand(
        closeMessageSubscriptionCommand.getSubscriptionPartitionId(),
        ValueType.MESSAGE_SUBSCRIPTION,
        MessageSubscriptionIntent.DELETE,
        messageSubscriptionRecord,
        logStreamRecordWriter);
  }

  private boolean onCloseProcessMessageSubscription(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    closeProcessMessageSubscriptionCommand.wrap(buffer, offset, length);

    final long processInstanceKey = closeProcessMessageSubscriptionCommand.getProcessInstanceKey();
    final int processInstancePartitionId = Protocol.decodePartitionId(processInstanceKey);

    processMessageSubscriptionRecord.reset();
    processMessageSubscriptionRecord
        .setSubscriptionPartitionId(
            closeProcessMessageSubscriptionCommand.getSubscriptionPartitionId())
        .setProcessInstanceKey(processInstanceKey)
        .setElementInstanceKey(closeProcessMessageSubscriptionCommand.getElementInstanceKey())
        .setMessageKey(-1)
        .setMessageName(closeProcessMessageSubscriptionCommand.getMessageName());

    return writeCommand(
        processInstancePartitionId,
        ValueType.PROCESS_MESSAGE_SUBSCRIPTION,
        ProcessMessageSubscriptionIntent.DELETE,
        processMessageSubscriptionRecord,
        logStreamRecordWriter);
  }

  private boolean onRejectCorrelateMessageSubscription(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    resetMessageCorrelationCommand.wrap(buffer, offset, length);

    final long processInstanceKey = resetMessageCorrelationCommand.getProcessInstanceKey();

    messageSubscriptionRecord.reset();
    messageSubscriptionRecord
        .setProcessInstanceKey(processInstanceKey)
        .setElementInstanceKey(-1L)
        .setBpmnProcessId(resetMessageCorrelationCommand.getBpmnProcessId())
        .setMessageName(resetMessageCorrelationCommand.getMessageName())
        .setCorrelationKey(resetMessageCorrelationCommand.getCorrelationKey())
        .setMessageKey(resetMessageCorrelationCommand.getMessageKey())
        .setInterrupting(false);

    return writeCommand(
        resetMessageCorrelationCommand.getSubscriptionPartitionId(),
        ValueType.MESSAGE_SUBSCRIPTION,
        MessageSubscriptionIntent.REJECT,
        messageSubscriptionRecord,
        logStreamRecordWriter);
  }

  private boolean writeCommand(
      final int partitionId,
      final ValueType valueType,
      final Intent intent,
      final UnpackedObject command,
      final LogStreamRecordWriter logStreamRecordWriter) {
    logStreamRecordWriter.reset();
    recordMetadata.reset().recordType(RecordType.COMMAND).valueType(valueType).intent(intent);

    final long position =
        logStreamRecordWriter
            .key(-1)
            .metadataWriter(recordMetadata)
            .valueWriter(command)
            .tryWrite();

    return position > 0;
  }

  @Override
  public void apply(
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final LogStreamRecordWriter logStreamRecordWriter) {
    messageHeaderDecoder.wrap(buffer, offset);

    if (messageHeaderDecoder.schemaId() == OpenMessageSubscriptionDecoder.SCHEMA_ID) {

      switch (messageHeaderDecoder.templateId()) {
        case OpenMessageSubscriptionDecoder.TEMPLATE_ID:
          onOpenMessageSubscription(buffer, offset, length, logStreamRecordWriter);
          break;
        case OpenProcessMessageSubscriptionDecoder.TEMPLATE_ID:
          onOpenProcessMessageSubscription(buffer, offset, length, logStreamRecordWriter);
          break;
        case CorrelateProcessMessageSubscriptionDecoder.TEMPLATE_ID:
          onCorrelateProcessMessageSubscription(buffer, offset, length, logStreamRecordWriter);
          break;
        case CorrelateMessageSubscriptionDecoder.TEMPLATE_ID:
          onCorrelateMessageSubscription(buffer, offset, length, logStreamRecordWriter);
          break;
        case CloseMessageSubscriptionDecoder.TEMPLATE_ID:
          onCloseMessageSubscription(buffer, offset, length, logStreamRecordWriter);
          break;
        case CloseProcessMessageSubscriptionDecoder.TEMPLATE_ID:
          onCloseProcessMessageSubscription(buffer, offset, length, logStreamRecordWriter);
          break;
        case RejectCorrelateMessageSubscriptionDecoder.TEMPLATE_ID:
          onRejectCorrelateMessageSubscription(buffer, offset, length, logStreamRecordWriter);
          break;
        default:
          break;
      }
    }
  }
}
