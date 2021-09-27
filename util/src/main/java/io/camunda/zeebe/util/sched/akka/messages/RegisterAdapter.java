package io.camunda.zeebe.util.sched.akka.messages;

import akka.actor.typed.ActorRef;
import akka.pattern.StatusReply;
import io.camunda.zeebe.util.sched.akka.ActorAdapter;
import java.util.Objects;

public final class RegisterAdapter implements Message {
  private final ActorRef<StatusReply<Void>> replyTo;
  private final ActorAdapter adapter;

  public RegisterAdapter(final ActorRef<StatusReply<Void>> replyTo, final ActorAdapter adapter) {
    this.replyTo = Objects.requireNonNull(replyTo);
    this.adapter = Objects.requireNonNull(adapter);
  }

  public ActorRef<StatusReply<Void>> getReplyTo() {
    return replyTo;
  }

  public ActorAdapter getAdapter() {
    return adapter;
  }
}
