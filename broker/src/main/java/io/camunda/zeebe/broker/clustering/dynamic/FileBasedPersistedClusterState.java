/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.dynamic;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileBasedPersistedClusterState implements LocalPersistedClusterState {
  private Cluster cluster;
  private final Path configFile;

  public FileBasedPersistedClusterState(final Path configFile) {
    this.configFile = configFile;
    try {
      initialize();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  void initialize() throws IOException {
    if (!Files.exists(configFile)) {
      cluster = null;
      return;
    }
    final byte[] bytes = Files.readAllBytes(configFile);
    if (bytes.length > 0) {
      cluster = Cluster.decode(bytes);
    } else {
      cluster = null;
    }
  }

  @Override
  public Cluster getClusterState() {
    return cluster;
  }

  @Override
  public void setClusterState(final Cluster cluster) {
    this.cluster = cluster;
    try {
      Files.write(
          configFile, cluster.encode(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
