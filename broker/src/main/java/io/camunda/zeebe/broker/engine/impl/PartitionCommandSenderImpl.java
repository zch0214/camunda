/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.engine.impl;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.camunda.zeebe.broker.partitioning.topology.TopologyPartitionListenerImpl;
import io.camunda.zeebe.clustering.management.InterPartitionCommandMetaDataEncoder;
import io.camunda.zeebe.clustering.management.MessageHeaderEncoder;
import io.camunda.zeebe.engine.processing.message.command.PartitionCommandSender;
import io.camunda.zeebe.util.buffer.BufferWriter;
import java.util.function.LongSupplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.UnsafeBuffer;

public final class PartitionCommandSenderImpl implements PartitionCommandSender {

  private final ClusterCommunicationService communicationService;

  private final int metadataEncodingLength;
  private final LongSupplier checkpointIdSupplier;
  private final TopologyPartitionListenerImpl partitionListener;

  public PartitionCommandSenderImpl(
      final ClusterCommunicationService communicationService,
      final LongSupplier checkpointIdSupplier,
      final TopologyPartitionListenerImpl partitionListener) {
    this.communicationService = communicationService;
    this.checkpointIdSupplier = checkpointIdSupplier;
    this.partitionListener = partitionListener;
    metadataEncodingLength =
        new MessageHeaderEncoder().encodedLength()
            + new InterPartitionCommandMetaDataEncoder().sbeBlockLength();
  }

  @Override
  public boolean sendCommand(
      final int receiverPartitionId, final BufferWriter command, final String commandType) {
    final Int2IntHashMap partitionLeaders = partitionListener.getPartitionLeaders();
    if (!partitionLeaders.containsKey(receiverPartitionId)) {
      return true;
    }
    final int partitionLeader = partitionLeaders.get(receiverPartitionId);

    final InterPartitionCommandMetaDataEncoder metadataEncoder =
        new InterPartitionCommandMetaDataEncoder();

    final byte[] bytes = new byte[metadataEncodingLength + command.getLength()];
    final MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
    metadataEncoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
    metadataEncoder.checkpointId(checkpointIdSupplier.getAsLong()).partitionId(receiverPartitionId);
    command.write(buffer, metadataEncodingLength);

    communicationService.unicast(commandType, bytes, MemberId.from("" + partitionLeader));
    return true;
  }
}
