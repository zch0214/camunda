/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.virtual;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import jdk.incubator.concurrent.StructuredTaskScope;
import org.junit.jupiter.api.Test;

final class VirtualActorSchedulerTest {
  @Test
  void shouldScheduleActor() {
    // given
    try (final var scheduler = new VirtualActorScheduler()) {
      final var actor = scheduler.schedule((ignored, error) -> {});
      final var holder = new IntegerHolder();

      // when
      actor.suspend();
      actor.execute(() -> holder.value++);
      actor.execute(() -> holder.value++);
      actor.execute(() -> holder.value++);
      final var result = actor.submit(() -> holder.value++);
      actor.resume();

      // then
      assertThat(result).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(3);
    }
  }

  @Test
  void shouldExecuteTasksSequentially() {
    // given
    final var list = new CopyOnWriteArrayList<>();
    try (final var scheduler = new VirtualActorScheduler()) {
      final var actor = scheduler.schedule((ignored, error) -> {});
      final var actor2 = scheduler.schedule((ignored, error) -> {});

      // when
      actor.suspend();
      actor.execute(() -> list.add(1));
      actor.execute(() -> list.add(2));
      actor.execute(() -> list.add(3));
      actor.submit(() -> list.add(4));
      actor.resume();

      // then
      assertThat(list).containsExactly(1, 2, 3, 4);
    }
  }

  @Test
  void shouldWorkWithStructuredConcurrency()
      throws InterruptedException, TimeoutException, ExecutionException {
    try (final var scheduler = new VirtualActorScheduler()) {
      final var actor = scheduler.schedule((ignored, error) -> {});
      final var actor2 = scheduler.schedule((ignored, error) -> {});
      try (final var scope = new StructuredTaskScope.ShutdownOnFailure()) {
        actor.close();

        final Future<Integer> getOne = scope.fork(() -> actor.submit(() -> 1).get());
        final Future<Integer> getTwo = scope.fork(() -> actor2.submit(() -> 2).get());

        scope.joinUntil(Instant.now().plus(Duration.ofSeconds(10)));

        if (scope.exception().isPresent()) {
          System.out.println("Failure: " + scope.exception().get());
        }
      }
    }
  }

  @Test
  void shouldIdleWhenNoTasks() throws InterruptedException {
    // given
    try (final var scheduler = new VirtualActorScheduler()) {
      final var actor = scheduler.schedule((ignored, error) -> {});

      // when
      Thread.sleep(10_000);

      // then
    }
  }

  private static final class IntegerHolder {
    private int value = 0;
  }
}
