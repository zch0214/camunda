/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.engine.impl;

import io.camunda.zeebe.broker.Loggers;
import io.camunda.zeebe.broker.partitioning.topology.TopologyPartitionListenerImpl;
import io.camunda.zeebe.broker.system.management.deployment.PushDeploymentRequest;
import io.camunda.zeebe.broker.system.management.deployment.PushDeploymentResponse;
import io.camunda.zeebe.engine.processing.deployment.DeploymentResponder;
import io.camunda.zeebe.engine.processing.deployment.distribute.DeploymentDistributor;
import io.camunda.zeebe.engine.processing.message.command.PartitionCommandSender;
import io.camunda.zeebe.protocol.impl.encoding.ErrorResponse;
import io.camunda.zeebe.util.sched.ActorControl;
import io.camunda.zeebe.util.sched.future.ActorFuture;
import io.camunda.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.slf4j.Logger;

public final class DeploymentDistributorImpl implements DeploymentDistributor, DeploymentResponder {

  public static final Duration PUSH_REQUEST_TIMEOUT = Duration.ofSeconds(15);
  public static final Duration RETRY_DELAY = Duration.ofMillis(100);
  private static final Logger LOG = Loggers.PROCESS_REPOSITORY_LOGGER;
  private static final String DEPLOYMENT_PUSH_TOPIC = "deployment";
  private final PushDeploymentResponse pushDeploymentResponse = new PushDeploymentResponse();

  private final ErrorResponse errorResponse = new ErrorResponse();

  private final TopologyPartitionListenerImpl partitionListener;

  private final PartitionCommandSender partitionCommandSender;
  private final ActorControl actor;

  public DeploymentDistributorImpl(
      final TopologyPartitionListenerImpl partitionListener,
      final PartitionCommandSender partitionCommandSender,
      final ActorControl actor) {

    this.partitionListener = partitionListener;
    this.partitionCommandSender = partitionCommandSender;
    this.actor = actor;
  }

  @Override
  public ActorFuture<Void> pushDeploymentToPartition(
      final long key, final int partitionId, final DirectBuffer deploymentBuffer) {
    final var pushedFuture = new CompletableActorFuture<Void>();

    LOG.debug("Distribute deployment {} to partition {}.", key, partitionId);
    final PushDeploymentRequest pushRequest =
        new PushDeploymentRequest().deployment(deploymentBuffer).deploymentKey(key);

    scheduleRetryPushDeploymentAfterADelay(partitionId, pushedFuture, pushRequest);
    sendPushDeploymentRequest(partitionId, pushedFuture, pushRequest);

    return pushedFuture;
  }

  private void scheduleRetryPushDeploymentAfterADelay(
      final int partitionId,
      final CompletableActorFuture<Void> pushedFuture,
      final PushDeploymentRequest pushRequest) {
    actor.runDelayed(
        PUSH_REQUEST_TIMEOUT,
        () -> {
          final String topic = getDeploymentResponseTopic(pushRequest.deploymentKey(), partitionId);
          if (!pushedFuture.isDone()) {
            LOG.warn(
                "Failed to receive deployment response for partition {} (on topic '{}'). Retrying",
                partitionId,
                topic);

            scheduleRetryPushDeploymentAfterADelay(partitionId, pushedFuture, pushRequest);
            sendPushDeploymentRequest(partitionId, pushedFuture, pushRequest);
          }
        });
  }

  private void sendPushDeploymentRequest(
      final int partitionId,
      final CompletableActorFuture<Void> pushedFuture,
      final PushDeploymentRequest pushRequest) {
    final Int2IntHashMap currentPartitionLeaders = partitionListener.getPartitionLeaders();
    if (currentPartitionLeaders.containsKey(partitionId)) {
      final int leader = currentPartitionLeaders.get(partitionId);
      pushDeploymentToPartition(leader, partitionId, pushRequest);
    }
  }

  private void pushDeploymentToPartition(
      final int partitionLeaderId, final int partition, final PushDeploymentRequest pushRequest) {
    pushRequest.partitionId(partition);

    final var send =
        partitionCommandSender.sendCommand(partition, pushRequest, DEPLOYMENT_PUSH_TOPIC);

    // TODO: Error handling

    //    pushDeploymentFuture.whenComplete(
    //        (response, throwable) -> {
    //          if (throwable != null) {
    //            LOG.warn(
    //                "Failed to push deployment to node {} for partition {}",
    //                partitionLeaderId,
    //                partition,
    //                throwable);
    //            handleRetry(partitionLeaderId, partition, pushRequest);
    //          } else {
    //            final DirectBuffer responseBuffer = new UnsafeBuffer(response);
    //            if (errorResponse.tryWrap(responseBuffer)) {
    //              handleErrorResponseOnPushDeploymentRequest(
    //                  partitionLeaderId, partition, pushRequest, responseBuffer);
    //            }
    //          }
    //        });
  }

  public static String getDeploymentResponseTopic(final long deploymentKey, final int partitionId) {
    return String.format("deployment-response-%d-%d", deploymentKey, partitionId);
  }

  @Override
  public void sendDeploymentResponse(final long deploymentKey, final int partitionId) {
    final PushDeploymentResponse deploymentResponse = new PushDeploymentResponse();
    deploymentResponse.deploymentKey(deploymentKey).partitionId(partitionId);
    final String topic =
        DeploymentDistributorImpl.getDeploymentResponseTopic(deploymentKey, partitionId);

    partitionCommandSender.sendCommand(partitionId, deploymentResponse, topic);
    LOG.trace("Send deployment response on topic {} for partition {}", topic, partitionId);
  }
}
