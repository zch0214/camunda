/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.clustering;

import io.camunda.zeebe.it.util.GrpcClientRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class BackupTest {

  public final Timeout testTimeout = Timeout.seconds(120);
  public final ClusteringRule clusteringRule = new ClusteringRule(3, 1, 1);
  public final GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  @Test
  public void shouldTriggerBackup() {
    // given

    // when
    clusteringRule.sendCheckpointCommand(1, 1);
    clusteringRule.sendCheckpointCommand(2, 1);
    clusteringRule.sendCheckpointCommand(2, 1);
  }

  @Test
  public void shouldTriggerBackupByRemoteCommand() {
    // given

    // when
    clusteringRule.sendCheckpointCommand(1, 1);
    clientRule.createSingleJob("Test"); // deploys
  }

  @Test
  public void shouldTriggerBackupMultiPartitions() {
    // given

    // when
    clusteringRule.sendCheckpointCommand(1, 1);
    clusteringRule.sendCheckpointCommand(1, 2);
    clientRule.createSingleJob("Test"); // deploys
  }
}
