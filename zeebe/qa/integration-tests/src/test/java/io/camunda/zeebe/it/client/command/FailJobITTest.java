/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.client.command;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.broker.test.EmbeddedBrokerRule;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.it.util.GrpcClientRule;
import io.camunda.zeebe.test.util.BrokerClassRuleHelper;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class FailJobITTest {

  private static final EmbeddedBrokerRule BROKER_RULE =
      new EmbeddedBrokerRule(brokerCfg -> brokerCfg.getThreads().setCpuThreadCount(3));
  private static final GrpcClientRule CLIENT_RULE = new GrpcClientRule(BROKER_RULE);

  @ClassRule
  public static RuleChain ruleChain = RuleChain.outerRule(BROKER_RULE).around(CLIENT_RULE);

  @Rule public final BrokerClassRuleHelper helper = new BrokerClassRuleHelper();

  private String jobType;
  private long jobKey;
  private final List<Long> completedJobs = new ArrayList<>();

  @Before
  public void init() {
    jobType = helper.getJobType();
    int i = 0;
    while (i < 10) {
      CLIENT_RULE.createSingleJob(jobType);
      i++;
    }
  }

  @Test
  public void test() {
    // when
    final Duration backoffTimeout = Duration.ofSeconds(10);
    final var start = LocalDateTime.now();
    while (true) {
      final var job = activateJob();

      if (job.isPresent()) {
        CLIENT_RULE
            .getClient()
            .newFailCommand(job.get().getKey())
            .retries(3)
            .retryBackoff(backoffTimeout)
            .send()
            .join();
      }
      final var now = LocalDateTime.now();
      if (start.plusMinutes(3).isBefore(now)) {
        break;
      }
    }

    Awaitility.await()
        .timeout(Duration.ofSeconds(40))
        .untilAsserted(
            () -> {
              activateAndComplete10Jobs();
              assertThat(completedJobs.size()).isEqualTo(10);
            });
  }

  private Optional<ActivatedJob> activateJob() {
    final var activateResponse =
        CLIENT_RULE
            .getClient()
            .newActivateJobsCommand()
            .jobType(jobType)
            .maxJobsToActivate(1)
            .timeout(Duration.ofMinutes(2))
            .send()
            .join();
    if (activateResponse.getJobs().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(activateResponse.getJobs().get(0));
  }

  private void activateAndComplete10Jobs() {
    final var activateResponse =
        CLIENT_RULE
            .getClient()
            .newActivateJobsCommand()
            .jobType(jobType)
            .maxJobsToActivate(10)
            .send()
            .join();

    activateResponse
        .getJobs()
        .forEach(
            job -> {
              CLIENT_RULE.getClient().newCompleteCommand(job.getKey()).send().join();
              completedJobs.add(job.getKey());
            });
  }
}
