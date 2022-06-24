/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.restore;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.Partition;
import io.atomix.raft.partition.RaftPartition;
import io.camunda.zeebe.backup.LocalFileSystemBackupStore;
import io.camunda.zeebe.broker.Loggers;
import io.camunda.zeebe.broker.partitioning.RaftPartitionGroupFactory;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.configuration.DataCfg;
import io.camunda.zeebe.journal.file.SegmentedJournal;
import io.camunda.zeebe.shared.Profile;
import io.camunda.zeebe.snapshots.ReceivableSnapshotStore;
import io.camunda.zeebe.snapshots.ReceivableSnapshotStoreFactory;
import io.camunda.zeebe.snapshots.impl.FileBasedSnapshotStore;
import io.camunda.zeebe.snapshots.impl.FileBasedSnapshotStoreFactory;
import io.camunda.zeebe.snapshots.impl.SnapshotMetrics;
import io.camunda.zeebe.util.FileUtil;
import io.camunda.zeebe.util.error.FatalErrorHandler;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(
    scanBasePackages = {"io.camunda.zeebe.restore", "io.camunda.zeebe.broker.system.configuration"})
@ConfigurationPropertiesScan(basePackages = {"io.camunda.zeebe.broker"})
public class RestoreApp implements CommandLineRunner {

  private final BrokerCfg configuration;

  @Autowired
  public RestoreApp(final BrokerCfg configuration) {
    this.configuration = configuration;
  }

  private void fixJournal(final RaftPartition partition, final long checkpointPosition) {

    final var journal =
        SegmentedJournal.builder()
            .withDirectory(partition.dataDirectory())
            .withName(partition.name())
            .build();
    final var reader = journal.openReader();
    reader.seekToAsqn(checkpointPosition);
    if (!reader.hasNext()) {
      // backup not valid
    }
    final var index = reader.next().index();
    journal.deleteAfter(index);
    reader.close();
    journal.close();
  }

  public static void main(final String[] args) {

    Thread.setDefaultUncaughtExceptionHandler(
        FatalErrorHandler.uncaughtExceptionHandler(Loggers.SYSTEM_LOGGER));

    final var application =
        new SpringApplicationBuilder(RestoreApp.class)
            .logStartupInfo(true)
            .profiles(Profile.RESTORE.getId())
            .build(args);

    application.run();
  }

  @Override
  public void run(final String... args) {
    try {
      restore();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void restore() throws Exception {

    final long checkpointId = 10;
    final Path backupPath = Paths.get("/tmp/fa11ea26-3307-46bd-839d-c3ce9f428f19");

    final RaftPartitionGroupFactory factor = new RaftPartitionGroupFactory();

    final DataCfg dataConfiguration = configuration.getData();
    final String rootDirectory = dataConfiguration.getDirectory();
    final var rootPath = Paths.get(rootDirectory);
    Files.createDirectories(rootPath);

    final int nodeId = configuration.getCluster().getNodeId();
    final SnapshotStoreFactory factory = new SnapshotStoreFactory(nodeId);
    final var partitionsGroup = factor.buildRaftPartitionGroup(configuration, factory);

    final var localMember = MemberId.from(String.valueOf(nodeId));
    for (final Partition partition : partitionsGroup.getPartitions()) {
      final RaftPartition p = (RaftPartition) partition;

      final var backupStore =
          new LocalFileSystemBackupStore(
              backupPath,
              configuration.getCluster().getNodeId(),
              partition.id().id(),
              factory.createSnapshotStore(p.dataDirectory().toPath(), partition.id().id(), nodeId));
      if (partition.members().contains(localMember)) {
        // restore
        Files.createDirectories(p.dataDirectory().toPath());
        backupStore.loadBackup(checkpointId).restore(p.dataDirectory().toPath());
      }
    }

    for (final Partition partition : partitionsGroup.getPartitions()) {
      final RaftPartition p = (RaftPartition) partition;
      final var checkpointPosition = Long.MAX_VALUE; // TODO: Read checkpointPosition from backup
      fixJournal(p, checkpointPosition);
    }
  }

  class SnapshotStoreFactory implements ReceivableSnapshotStoreFactory {

    private final int nodeId;
    private FileBasedSnapshotStore snapshotStore;

    SnapshotStoreFactory(final int nodeId) {
      this.nodeId = nodeId;
    }

    private FileBasedSnapshotStore createSnapshotStore(
        final Path root, final int partitionId, final int nodeId) {
      final var snapshotDirectory = root.resolve(FileBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY);
      final var pendingDirectory = root.resolve(FileBasedSnapshotStoreFactory.PENDING_DIRECTORY);

      try {
        FileUtil.ensureDirectoryExists(snapshotDirectory);
        FileUtil.ensureDirectoryExists(pendingDirectory);
      } catch (final IOException e) {
        throw new UncheckedIOException("Failed to create snapshot directories", e);
      }

      return new FileBasedSnapshotStore(
          nodeId,
          partitionId,
          new SnapshotMetrics(Integer.toString(partitionId)),
          snapshotDirectory,
          pendingDirectory);
    }

    @Override
    public ReceivableSnapshotStore createReceivableSnapshotStore(
        final Path directory, final int partitionId) {
      snapshotStore = createSnapshotStore(directory, partitionId, nodeId);
      return snapshotStore;
    }
  }
}
