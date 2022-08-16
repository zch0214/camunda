/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.backup.api.Backup;
import io.camunda.zeebe.backup.api.BackupIdentifier;
import io.camunda.zeebe.backup.api.BackupStatus;
import io.camunda.zeebe.backup.api.BackupStore;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class S3BackupStore implements BackupStore {

  MinioClient minioClient =
      MinioClient.builder()
          .credentials("admin", "admin1234")
          .endpoint("http://172.17.0.2:9000")
          .build();
  private final String bucket = "backup";

  @Override
  public CompletableFuture<Void> save(final Backup backup) {

    /**
     * Metadata { backupId : checkpointId: nodeId: partitionId: snapshotId : numberOfPartitions :
     * checkpointPosition : snapshotFiles : [string] segmentFiles : [string] }
     *
     * <p>pid/cid/noid/metadata
     *
     * <p>cid/pid/noid/snapshot/sfile0 cid/pid/noid/snapshot/sfile1
     *
     * <p>cid/pid/noid/segments/file0
     *
     * <p>Status = ONGOING | COMPLETED | FAILED pid/cid/noid/status/
     * cid/pid/noid/status/failureReason
     */
    try {

      final Metadata metadata = new Metadata(1, 1, 1, 1, "id", 1, List.of("test"), List.of("logs"));

      final ObjectMapper objectMapper = new ObjectMapper();

      final ByteArrayOutputStream outputstream = new ByteArrayOutputStream();
      objectMapper.writer().writeValue(outputstream, metadata);
      final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputstream.toByteArray());

      minioClient.putObject(
          PutObjectArgs.builder().bucket(bucket).object("pid/cid/nid/metadata").stream(
                  inputStream, outputstream.size(), -1)
              .build());

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public CompletableFuture<BackupStatus> getStatus(final BackupIdentifier id) {
    return null;
  }

  @Override
  public CompletableFuture<Void> delete(final BackupIdentifier id) {
    return null;
  }

  @Override
  public CompletableFuture<Backup> restore(final BackupIdentifier id) {
    return null;
  }

  @Override
  public CompletableFuture<Void> markFailed(final BackupIdentifier id) {
    return null;
  }
}
