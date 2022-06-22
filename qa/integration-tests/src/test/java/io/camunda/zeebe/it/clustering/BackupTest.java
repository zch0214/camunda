/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.clustering;

import io.camunda.zeebe.broker.system.partitions.impl.steps.BackupStorePartitionTransitionStep;
import io.camunda.zeebe.it.util.GrpcClientRule;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.slf4j.LoggerFactory;

public class BackupTest {

  public final Timeout testTimeout = Timeout.seconds(120);
  public final ClusteringRule clusteringRule = new ClusteringRule(3, 3, 3);
  public final GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  @Test
  public void shouldTriggerBackup() throws InterruptedException, IOException {
    // given
    publishMessages();
    clusteringRule.getBrokers().forEach(clusteringRule::takeSnapshot);
    publishMessages();
    // when
    Thread.sleep(10000); // to wait until snapshot exists
    clusteringRule.sendCheckpointCommand(10, 1);
    clusteringRule.sendCheckpointCommand(10, 2);
    clusteringRule.sendCheckpointCommand(10, 3);
    publishMessages();

    clusteringRule.sendCheckpointCommand(20, 1);
    clusteringRule.sendCheckpointCommand(20, 2);
    clusteringRule.sendCheckpointCommand(20, 3);

    Thread.sleep(10000); // to wait until snapshot exists

    final var log = LoggerFactory.getLogger("TEST:BACKUP");
    final StringBuilder outputBuilder = new StringBuilder();
    Files.walk(BackupStorePartitionTransitionStep.BACKUP_ROOT_DIRECTORY)
        .forEachOrdered(p -> outputBuilder.append("\n").append(p));
    log.info("{}", outputBuilder);
  }

  private void publishMessages() {
    clientRule
        .getClient()
        .newPublishMessageCommand()
        .messageName("abc")
        .correlationKey("1")
        .send()
        .join();
    clientRule
        .getClient()
        .newPublishMessageCommand()
        .messageName("abc")
        .correlationKey("2")
        .send()
        .join();
    clientRule
        .getClient()
        .newPublishMessageCommand()
        .messageName("abc")
        .correlationKey("3")
        .send()
        .join();
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
