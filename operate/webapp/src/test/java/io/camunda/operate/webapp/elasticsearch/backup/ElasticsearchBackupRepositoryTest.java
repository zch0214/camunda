/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.operate.webapp.elasticsearch.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.operate.property.OperateProperties;
import io.camunda.operate.webapp.backup.Metadata;
import io.camunda.operate.webapp.management.dto.BackupStateDto;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.SnapshotClient;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ElasticsearchBackupRepositoryTest {

  @Mock RestHighLevelClient esClient;

  OperateProperties operateProperties = new OperateProperties();

  ObjectMapper objectMapper = new ObjectMapper();

  ElasticsearchBackupRepository repository;

  @BeforeEach
  void setUp() {
    repository = new ElasticsearchBackupRepository(esClient, objectMapper, operateProperties);
  }

  @Test
  void shouldCreate() {
    assertThat(repository).isNotNull();
  }

  @Test
  void shouldReturnBackupStateCompleted() throws IOException {
    final var snapshotClient = mock(SnapshotClient.class);
    final var firstSnapshotInfo = mock(SnapshotInfo.class);
    final var snapshotResponse = mock(GetSnapshotsResponse.class);

    // Set up Snapshot client
    when(esClient.snapshot()).thenReturn(snapshotClient);
    // Set up Snapshot details
    when(firstSnapshotInfo.userMetadata())
        .thenReturn(objectMapper.convertValue(new Metadata().setPartCount(1), Map.class));
    when(firstSnapshotInfo.snapshotId()).thenReturn(new SnapshotId("snapshot-name", "uuid"));
    when(firstSnapshotInfo.state()).thenReturn(SnapshotState.SUCCESS);

    // Set up Snapshot response
    when(snapshotResponse.getSnapshots()).thenReturn(List.of(firstSnapshotInfo));
    when(snapshotClient.get(any(), any())).thenReturn(snapshotResponse);

    // Test
    final var backupState = repository.getBackupState("repository-name", 5L);
    assertThat(backupState.getState()).isEqualTo(BackupStateDto.COMPLETED);
  }

  @Test
  void shouldReturnBackupStateIncomplete() throws IOException {
    final var snapshotClient = mock(SnapshotClient.class);
    final var firstSnapshotInfo = mock(SnapshotInfo.class);
    final var snapshotResponse = mock(GetSnapshotsResponse.class);

    // Set up Snapshot client
    when(esClient.snapshot()).thenReturn(snapshotClient);
    // Set up Snapshot details
    when(firstSnapshotInfo.userMetadata())
        .thenReturn(objectMapper.convertValue(new Metadata().setPartCount(3), Map.class));
    when(firstSnapshotInfo.snapshotId()).thenReturn(new SnapshotId("snapshot-name", "uuid"));
    when(firstSnapshotInfo.state()).thenReturn(SnapshotState.SUCCESS);

    // Set up Snapshot response
    when(snapshotResponse.getSnapshots()).thenReturn(List.of(firstSnapshotInfo));
    when(snapshotClient.get(any(), any())).thenReturn(snapshotResponse);

    // Test
    final var backupState = repository.getBackupState("repository-name", 5L);
    assertThat(backupState.getState()).isEqualTo(BackupStateDto.INCOMPLETE);
  }

  @Test
  void shouldReturnBackupStateProgress() throws IOException {
    final var snapshotClient = mock(SnapshotClient.class);
    final var firstSnapshotInfo = mock(SnapshotInfo.class);
    final var snapshotResponse = mock(GetSnapshotsResponse.class);

    // Set up Snapshot client
    when(esClient.snapshot()).thenReturn(snapshotClient);
    // Set up Snapshot details
    when(firstSnapshotInfo.userMetadata())
        .thenReturn(objectMapper.convertValue(new Metadata().setPartCount(3), Map.class));
    when(firstSnapshotInfo.snapshotId()).thenReturn(new SnapshotId("snapshot-name", "uuid"));
    when(firstSnapshotInfo.state()).thenReturn(SnapshotState.SUCCESS);
    when(firstSnapshotInfo.startTime()).thenReturn(Instant.now().toEpochMilli());

    // Set up Snapshot response
    when(snapshotResponse.getSnapshots()).thenReturn(List.of(firstSnapshotInfo));
    when(snapshotClient.get(any(), any())).thenReturn(snapshotResponse);

    // Test
    final var backupState = repository.getBackupState("repository-name", 5L);
    assertThat(backupState.getState()).isEqualTo(BackupStateDto.IN_PROGRESS);
  }
}
