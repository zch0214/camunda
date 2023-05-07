/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.api;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implementations of this interface must ensure that tasks scheduled to be executed at the exact
 * same time (note: depending on the underlying clock coarseness) will be executed in the order in
 * which they are submitted, one at a time.
 */
@FunctionalInterface
public interface SequentialScheduler {
  <T> ScheduledFuture<T> schedule(final Callable<T> task, final long delay, final TimeUnit unit);

  default ScheduledFuture<Void> schedule(
      final Runnable task, final long delay, final TimeUnit unit) {
    return schedule(
        () -> {
          task.run();
          return null;
        },
        delay,
        unit);
  }
}
