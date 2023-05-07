/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.api;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * A sequential context allows accessing data asynchronously, with the guarantee that all tasks
 * submitted are processed one at a time, in the order in which they are submitted.
 */
@FunctionalInterface
public interface SequentialContext {

  /**
   * Submits a callable which will be executing within the given context.
   *
   * @param task the task to execute
   * @return a future, which is completed when the task has been executed, has failed exceptionally,
   *     or will never be executed
   * @param <T> the result type
   */
  <T> Future<T> submit(final Callable<T> task);

  /**
   * Submits a runnable to be executed within the sequential context.
   *
   * @param task the task to execute
   * @return a future, which is completed when the task has been executed, has failed exceptionally,
   *     or will never be executed
   */
  default Future<Void> submit(final Runnable task) {
    return submit(
        () -> {
          task.run();
          return null;
        });
  }
}
