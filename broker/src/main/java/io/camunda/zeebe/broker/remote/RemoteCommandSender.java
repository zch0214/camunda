/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.remote;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.camunda.zeebe.broker.partitioning.topology.TopologyPartitionListenerImpl;
import io.camunda.zeebe.clustering.management.InterPartitionCommandMetaDataEncoder;
import io.camunda.zeebe.clustering.management.MessageHeaderEncoder;
import io.camunda.zeebe.util.buffer.BufferWriter;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.UnsafeBuffer;

public class RemoteCommandSender {

  private final TopologyPartitionListenerImpl partitionListener;
  private final ClusterCommunicationService communicationService;

  private final int metadataEncodingLength;
  private long checkpointId; // TODO, get it from streamprocessor

  public RemoteCommandSender(
      final TopologyPartitionListenerImpl partitionListener,
      final ClusterCommunicationService communicationService) {
    this.partitionListener = partitionListener;
    this.communicationService = communicationService;
    metadataEncodingLength = 0; // TODO Find the SBE encoded length
  }

  public void sendCommand(final int toPartition, final BufferWriter command) {

    final Int2IntHashMap partitionLeaders = partitionListener.getPartitionLeaders();
    if (!partitionLeaders.containsKey(toPartition)) {
      return;
    }
    final int partitionLeader = partitionLeaders.get(toPartition);

    final InterPartitionCommandMetaDataEncoder metadataEncoder =
        new InterPartitionCommandMetaDataEncoder();

    final byte bytes[] = new byte[metadataEncodingLength + command.getLength()];
    final MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
    metadataEncoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
    metadataEncoder.checkpointId(checkpointId).partitionId(toPartition);
    command.write(buffer, metadataEncodingLength);

    communicationService.unicast("subscription", bytes, MemberId.from("" + partitionLeader));
  }
}
