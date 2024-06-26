/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.monitoring.DiskSpaceUsageMonitorActor;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.testing.TestConcurrencyControl;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DiskSpaceUsageMonitorStepTest {
  private static final TestConcurrencyControl CONCURRENCY_CONTROL = new TestConcurrencyControl();
  private static final BrokerCfg TEST_BROKER_CONFIG = new BrokerCfg();
  private static final Duration TIME_OUT = Duration.ofSeconds(10);

  private BrokerStartupContext mockBrokerStartupContext;
  private ActorSchedulingService mockActorSchedulingService;
  private DiskSpaceUsageMonitorActor mockDiskSpaceUsageMonitor;

  private ActorFuture<BrokerStartupContext> future;

  private final DiskSpaceUsageMonitorStep sut = new DiskSpaceUsageMonitorStep();

  @BeforeEach
  void setUp() {
    mockBrokerStartupContext = mock(BrokerStartupContext.class);
    mockActorSchedulingService = mock(ActorSchedulingService.class);
    mockDiskSpaceUsageMonitor = mock(DiskSpaceUsageMonitorActor.class);

    when(mockBrokerStartupContext.getBrokerConfiguration()).thenReturn(TEST_BROKER_CONFIG);
    when(mockBrokerStartupContext.getConcurrencyControl()).thenReturn(CONCURRENCY_CONTROL);
    when(mockBrokerStartupContext.getActorSchedulingService())
        .thenReturn(mockActorSchedulingService);
    when(mockBrokerStartupContext.getDiskSpaceUsageMonitor()).thenReturn(mockDiskSpaceUsageMonitor);
    when(mockDiskSpaceUsageMonitor.closeAsync())
        .thenReturn(CONCURRENCY_CONTROL.completedFuture(null));

    when(mockActorSchedulingService.submitActor(any()))
        .thenReturn(CONCURRENCY_CONTROL.completedFuture(null));

    future = CONCURRENCY_CONTROL.createFuture();
  }

  @Test
  void shouldCompleteFutureOnStartup() {
    // when
    sut.startupInternal(mockBrokerStartupContext, CONCURRENCY_CONTROL, future);

    // then
    assertThat(future).succeedsWithin(TIME_OUT);
    assertThat(future.join()).isNotNull();
  }

  @Test
  void shouldScheduleDiskSpaceUsageMonitorOnStartup() {
    // when
    sut.startupInternal(mockBrokerStartupContext, CONCURRENCY_CONTROL, future);

    // then
    final var argumentCaptor = ArgumentCaptor.forClass(DiskSpaceUsageMonitorActor.class);
    verify(mockBrokerStartupContext).setDiskSpaceUsageMonitor(argumentCaptor.capture());
    verify(mockActorSchedulingService).submitActor(argumentCaptor.getValue());
  }

  @Test
  void shouldCompleteFutureOnShutdown() {
    // when
    sut.shutdownInternal(mockBrokerStartupContext, CONCURRENCY_CONTROL, future);

    // then
    assertThat(future).succeedsWithin(TIME_OUT);
    assertThat(future.join()).isNotNull();
  }

  @Test
  void shouldStopHealthCheckServiceOnShutdown() {
    // when
    sut.shutdownInternal(mockBrokerStartupContext, CONCURRENCY_CONTROL, future);
    await().until(future::isDone);

    // then
    verify(mockDiskSpaceUsageMonitor).closeAsync();
    verify(mockBrokerStartupContext).setDiskSpaceUsageMonitor(null);
  }
}
