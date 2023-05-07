/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.api;

@FunctionalInterface
public interface ActorScheduler {

  /**
   * Schedules an actor. Once this returns, the actor can now receive asynchronous calls. It is also
   * tracked by the scheduler, and will be closed once the scheduler is closed.
   */
  Actor schedule(final ErrorHandler errorHandler);

  interface ErrorHandler {
    void handleError(final Actor actor, final Throwable error);
  }
}
