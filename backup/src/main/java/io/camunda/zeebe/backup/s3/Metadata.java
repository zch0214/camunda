/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.s3;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;

@JsonSerialize
public record Metadata(
    long checkpointId,
    int partitionId,
    int nodeId,
    long checkpointPosition,
    String snapshotId,
    int numberOfPartitions,
    List<String> snapshotFiles,
    List<String> segmentFiles) {}
