/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.virtual;

import java.time.InstantSource;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class VirtualActorTask<T> extends FutureTask<T>
    implements Delayed, ScheduledFuture<T>, RunnableFuture<T> {

  private final long sequence;
  private final long deadlineMs;
  private final InstantSource clock;

  public VirtualActorTask(
      final Callable<T> callable,
      final long sequence,
      final long deadlineMs,
      final InstantSource clock) {
    super(callable);

    this.sequence = sequence;
    this.deadlineMs = deadlineMs;
    this.clock = clock;
  }

  public VirtualActorTask(
      final Runnable runnable,
      final long sequence,
      final long deadlineMs,
      final InstantSource clock) {
    super(runnable, null);

    this.sequence = sequence;
    this.deadlineMs = deadlineMs;
    this.clock = clock;
  }

  @Override
  public int compareTo(final Delayed o) {
    final var compareUnit = TimeUnit.NANOSECONDS;
    final int timeComparison = Long.compare(getDelay(compareUnit), o.getDelay(compareUnit));
    if (timeComparison == 0 && o instanceof VirtualActorTask<?> task) {
      return Long.compare(sequence, task.sequence);
    }

    return timeComparison;
  }

  @Override
  public long getDelay(final TimeUnit unit) {
    final var remainingMs = deadlineMs - clock.millis();
    return unit.convert(remainingMs, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void setException(final Throwable t) {
    super.setException(t);
  }
}
