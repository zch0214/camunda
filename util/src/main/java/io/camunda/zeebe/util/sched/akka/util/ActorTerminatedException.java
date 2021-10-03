package io.camunda.zeebe.util.sched.akka.util;

import akka.actor.typed.ActorRef;

public final class ActorTerminatedException extends RuntimeException {

  public ActorTerminatedException(final ActorRef<?> actor) {
    super(
        String.format(
            "The actor %s was unexpectedly terminated without going through all phases of its lifecycle",
            actor.path()));
  }
}
