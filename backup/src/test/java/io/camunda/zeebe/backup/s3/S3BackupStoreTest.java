/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.s3;

import io.camunda.zeebe.backup.api.Backup;
import io.camunda.zeebe.backup.api.BackupIdentifier;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class S3BackupStoreTest {

  @Test
  void testStore() {

    final S3BackupStore store = new S3BackupStore();

    final Backup backup =
        new Backup() {
          @Override
          public BackupIdentifier id() {
            return new BackupIdentifier() {
              @Override
              public int nodeId() {
                return 0;
              }

              @Override
              public int partitionId() {
                return 0;
              }

              @Override
              public long checkpointId() {
                return 0;
              }
            };
          }

          @Override
          public int numberOfPartitions() {
            return 0;
          }

          @Override
          public String snapshotId() {
            return null;
          }

          @Override
          public Map<String, Path> snapshot() {
            return null;
          }

          @Override
          public long checkpointPosition() {
            return 0;
          }

          @Override
          public Map<String, Path> segments() {
            return null;
          }
        };

    store.save(backup);
  }
}
