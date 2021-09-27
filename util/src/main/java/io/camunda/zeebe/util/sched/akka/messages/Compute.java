package io.camunda.zeebe.util.sched.akka.messages;

import akka.actor.typed.ActorRef;
import akka.pattern.StatusReply;
import java.util.Objects;
import java.util.concurrent.Callable;

public final class Compute<V> implements Message, Callable<V> {
  private final ActorRef<StatusReply<V>> replyTo;
  private final Callable<V> operation;

  public Compute(final ActorRef<StatusReply<V>> replyTo, final Callable<V> operation) {
    this.replyTo = Objects.requireNonNull(replyTo);
    this.operation = Objects.requireNonNull(operation);
  }

  @Override
  public V call() throws Exception {
    return operation.call();
  }

  @SuppressWarnings("java:S1452")
  public ActorRef<StatusReply<V>> getReplyTo() {
    return replyTo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getReplyTo(), operation);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Compute)) {
      return false;
    }
    final Compute<?> compute = (Compute<?>) o;
    return getReplyTo().equals(compute.getReplyTo()) && operation.equals(compute.operation);
  }

  @Override
  public String toString() {
    return "Compute{" + "replyTo=" + replyTo + ", operation=" + operation + '}';
  }
}
