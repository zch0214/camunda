package io.camunda.zeebe.util.sched.akka.util;

import akka.actor.typed.ActorRef;

public final class ActorFailedException extends RuntimeException {

  public ActorFailedException(final ActorRef<?> actorRef) {
    super(String.format("Actor %s explicitly failed via its control", actorRef.path()));
  }
}
