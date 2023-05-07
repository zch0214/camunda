/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.api;

/**
 * An actor instance allows encapsulating data access via series of sequenced operations. One can
 * execute tasks in the context of a virtual actor, query its state, or scheduled timed tasks, with
 * the guarantee that only one task is executed at a time, and tasks are executed in sequence.
 *
 * <p>NOTE: delayed tasks are obviously executed only when their delay is up, and as such, may be
 * executed after a non-delayed tasks was scheduled, even if it was submitted after. However, it is
 * guaranteed that delayed-tasks are executed in sequence relative to themselves. There is no
 * guarantee of fairness between interleaving delayed and non-delayed tasks.
 *
 * <p>An actor can be suspended, in which case it will not execute anything, but will keep accepting
 * new tasks. This can cause it to accumulate a lot of memory, so be careful with this behavior.
 * Call {@link #resume()} to resume execution.
 */
public interface Actor
    extends SequentialExecutor, SequentialScheduler, SequentialContext, AutoCloseable {

  /**
   * Suspends executing further tasks on this actor. This will not interrupt any currently executing
   * task. Does nothing if the actor is stopped.
   *
   * @return true if the actor is now suspended, false otherwise (e.g. it was stopped)
   */
  boolean suspend();

  /**
   * Resumes a suspended actor. Does nothing if the actor is stopped or not suspended.
   *
   * @return true if the actor is resumed, false otherwise (e.g. the actor is stopped)
   */
  boolean resume();

  /**
   * Returns true if the actor is stopped, false otherwise. A stopped actor will not accept new
   * tasks or execute any.
   */
  boolean isStopped();
}
