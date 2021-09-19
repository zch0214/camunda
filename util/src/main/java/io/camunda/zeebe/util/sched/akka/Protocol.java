package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.ActorRef;
import akka.pattern.StatusReply;
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

  final class Compute<V> implements Operation, Callable<V> {
    private final Callable<V> operation;
    private final ActorRef<StatusReply<Result<V>>> replyTo;

    public Compute(final Callable<V> operation, final ActorRef<StatusReply<Result<V>>> replyTo) {
      this.operation = operation;
      this.replyTo = replyTo;
    }

    public Callable<V> getOperation() {
      return operation;
    }

    public ActorRef<StatusReply<Result<V>>> getReplyTo() {
      return replyTo;
    }

    @Override
    public V call() throws Exception {
      return operation.call();
    }
  }

  interface Execute extends Operation, Runnable {}

  interface Message {}

  interface Operation extends Message {}
}
