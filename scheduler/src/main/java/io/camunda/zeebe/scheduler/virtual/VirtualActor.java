/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.virtual;

import io.camunda.zeebe.scheduler.api.Actor;
import io.camunda.zeebe.scheduler.api.ActorScheduler.ErrorHandler;
import java.time.Clock;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

public final class VirtualActor implements Actor, Runnable {
  private final TaskQueue<VirtualActorTask<?>> tasks = new TaskQueue<>();
  private final AtomicReference<VirtualActorState> state =
      new AtomicReference<>(VirtualActorState.IDLE);

  // if we exhausted this sequencer, and every task took exactly 1 micro seconds, then it would
  // take ~584942 years to execute them all. this is a rough estimate of course, since we could
  // possibly enqueue everything before we process it (e.g. the executor is suspended), but we would
  // most likely run out of memory before we exhaust this sequencer
  private final AtomicLong sequencer = new AtomicLong(Long.MIN_VALUE);

  private final ErrorHandler errorHandler;
  private final InstantSource clock;
  private final IdleStrategy idleStrategy;

  private volatile Thread thread;

  public VirtualActor(final ErrorHandler errorHandler) {
    this(errorHandler, Clock.systemDefaultZone(), new BackoffIdleStrategy());
  }

  public VirtualActor(
      final ErrorHandler errorHandler, final InstantSource clock, final IdleStrategy idleStrategy) {
    this.errorHandler = Objects.requireNonNull(errorHandler, "must specify an error handler");
    this.clock = Objects.requireNonNull(clock, "must specify a clock");
    this.idleStrategy = Objects.requireNonNull(idleStrategy, "must specify an idle strategy");
  }

  @Override
  public void run() {
    thread = Thread.currentThread();

    while (!thread.isInterrupted() && state.get() != VirtualActorState.STOPPED) {
      final var task = pollNextRunnableTask();
      if (task != null) {
        idleStrategy.reset();
        runTask(task);
      } else if (state.compareAndSet(VirtualActorState.BUSY, VirtualActorState.IDLE)) {
        idleStrategy.idle();
      } else if (state.get() == VirtualActorState.SUSPENDED) {
        // TODO: figure out how to park the thread instead
        idleStrategy.idle();
      }
    }
  }

  @Override
  public void close() {
    final var oldState = state.getAndSet(VirtualActorState.STOPPED);
    if (oldState == VirtualActorState.STOPPED) {
      return;
    }

    // notify everyone awaiting the result of tasks that they will never complete since the actor
    // is shutting down
    final var remainingTasks = new ArrayList<VirtualActorTask<?>>();
    tasks.drainTo(remainingTasks);
    remainingTasks.forEach(
        task -> task.setException(new CancellationException("Actor is shutting down")));

    if (thread != null) {
      // try interrupting the thread to speed up shutdown
      thread.interrupt();
    }
  }

  @Override
  public boolean suspend() {
    final var state =
        this.state.accumulateAndGet(
            VirtualActorState.SUSPENDED,
            (oldState, newState) -> oldState == VirtualActorState.STOPPED ? oldState : newState);

    return state == VirtualActorState.SUSPENDED;
  }

  @Override
  public boolean resume() {
    if (!state.compareAndSet(VirtualActorState.SUSPENDED, VirtualActorState.IDLE)) {
      return false;
    }

    if (!tasks.isEmpty()) {
      scheduleExecution();
    }

    return true;
  }

  @Override
  public boolean isStopped() {
    return state.get() == VirtualActorState.STOPPED;
  }

  @Override
  public void execute(final Runnable task) {
    enqueueTask(newTaskFor(task));
  }

  @Override
  public <T> ScheduledFuture<T> schedule(
      final Callable<T> callable, final long delay, final TimeUnit unit) {
    final var adaptedTask =
        new VirtualActorTask<>(
            Objects.requireNonNull(callable, "must specify task"),
            sequencer.getAndIncrement(),
            clock.millis() + TimeUnit.MILLISECONDS.convert(delay, unit),
            clock);
    return enqueueTask(adaptedTask);
  }

  @Override
  public ScheduledFuture<Void> schedule(
      final Runnable command, final long delay, final TimeUnit unit) {
    final var adaptedTask =
        new VirtualActorTask<Void>(
            Objects.requireNonNull(command, "must specify task"),
            sequencer.getAndIncrement(),
            clock.millis() + TimeUnit.MILLISECONDS.convert(delay, unit),
            clock);
    return enqueueTask(adaptedTask);
  }

  @Override
  public <T> Future<T> submit(final Callable<T> task) {
    return enqueueTask(newTaskFor(task));
  }

  @Override
  public Future<Void> submit(final Runnable task) {
    return enqueueTask(newTaskFor(task));
  }

  private void runTask(final VirtualActorTask<?> task) {
    try {
      task.run();
    } catch (final Throwable e) {
      errorHandler.handleError(this, e);
    }
  }

  private VirtualActorTask<?> pollNextRunnableTask() {
    // we need to handle spurious executions with no tasks AND tasks which may have been cancelled
    // since the last scheduling
    VirtualActorTask<?> task;
    do {
      task = tasks.poll();
    } while (task != null && (task.isDone() || task.isCancelled()));
    return task;
  }

  private <T> VirtualActorTask<T> newTaskFor(final Runnable runnable) {
    return newTaskFor(
        () -> {
          runnable.run();
          return null;
        });
  }

  private <T> VirtualActorTask<T> newTaskFor(final Callable<T> callable) {
    return new VirtualActorTask<T>(
        Objects.requireNonNull(callable, "must specify task"),
        sequencer.getAndIncrement(),
        0,
        clock);
  }

  private <T> VirtualActorTask<T> enqueueTask(final VirtualActorTask<T> task) {
    Exception rejection = null;
    if (state.get() == VirtualActorState.STOPPED) {
      rejection = new RejectedExecutionException("Cannot execute task as the actor is closed");
    } else if (!tasks.offer(task)) {
      rejection = new RejectedExecutionException("Cannot execute task as the task queue is full");
    }

    if (state.get() == VirtualActorState.STOPPED) {
      tasks.remove(task);
      rejection = new RejectedExecutionException("Cannot execute task as the actor is closed");
    }

    if (rejection != null) {
      task.setException(rejection);
    } else {
      scheduleExecution();
    }

    return task;
  }

  private void scheduleExecution() {
    if (state.compareAndSet(VirtualActorState.IDLE, VirtualActorState.BUSY)) {
      idleStrategy.reset();
    }
  }
}
