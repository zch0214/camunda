/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.virtual;

import io.camunda.zeebe.scheduler.api.Actor;
import io.camunda.zeebe.scheduler.api.ActorScheduler;
import io.camunda.zeebe.util.error.FatalErrorHandler;
import java.time.Clock;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VirtualActorScheduler implements ActorScheduler, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(VirtualActorScheduler.class);

  private final Collection<VirtualActor> actors = new ArrayList<>(64);
  private final FatalErrorHandler fatalErrorHandler = FatalErrorHandler.withLogger(LOGGER);

  private final InstantSource clock;
  private final ExecutorService executorService;
  private final Supplier<IdleStrategy> idleStrategySupplier;

  public VirtualActorScheduler() {
    this(Thread.ofVirtual().name("actor-", 0).factory(), Clock.systemDefaultZone());
  }

  public VirtualActorScheduler(final ThreadFactory factory, final InstantSource clock) {
    this(Executors.newThreadPerTaskExecutor(factory), clock, BackoffIdleStrategy::new);
  }

  VirtualActorScheduler(
      final ExecutorService executorService,
      final InstantSource clock,
      final Supplier<IdleStrategy> idleStrategySupplier) {
    this.executorService =
        Objects.requireNonNull(executorService, "must specify an executor service");
    this.clock = Objects.requireNonNull(clock, "must specify a clock");
    this.idleStrategySupplier =
        Objects.requireNonNull(idleStrategySupplier, "must specify an idle strategy factory");
  }

  @Override
  public VirtualActor schedule(final ErrorHandler errorHandler) {
    final var decoratedErrorHandler = new FatalActorErrorHandler(fatalErrorHandler, errorHandler);
    final var actor = new VirtualActor(decoratedErrorHandler, clock, idleStrategySupplier.get());

    actors.add(actor);
    executorService.execute(actor);

    return actor;
  }

  public void close() {
    CloseHelper.closeAll(error -> {}, actors);
    executorService.close();
  }

  private record FatalActorErrorHandler(FatalErrorHandler fatalErrorHandler, ErrorHandler delegate)
      implements ErrorHandler {
    @Override
    public void handleError(final Actor actor, final Throwable error) {
      fatalErrorHandler.handleError(error);
      delegate.handleError(actor, error);
    }
  }
}
