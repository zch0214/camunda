package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.ActorRef;
import java.io.Serializable;
import java.util.concurrent.Callable;

public interface Protocol {
  final class Result<V> implements Message {
    private final V value;

    public Result(final V value) {
      this.value = value;
    }

    public V getValue() {
      return value;
    }
  }

  final class Callback<V> implements Operation {
    private final SerializableCallable<V> operation;
    private final ActorRef<Result<V>> replyTo;

    public Callback(final SerializableCallable<V> operation, final ActorRef<Result<V>> replyTo) {
      this.operation = operation;
      this.replyTo = replyTo;
    }

    public SerializableCallable<V> getOperation() {
      return operation;
    }

    public ActorRef<Result<V>> getReplyTo() {
      return replyTo;
    }
  }

  interface Message {}

  interface Operation extends Message {}

  @FunctionalInterface
  interface SerializableRunnable extends Runnable, Serializable, Operation {}

  @FunctionalInterface
  interface SerializableCallable<V> extends Callable<V>, Serializable, Operation {}
}
