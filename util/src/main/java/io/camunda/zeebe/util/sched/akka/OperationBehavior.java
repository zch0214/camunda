package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import io.camunda.zeebe.util.sched.akka.Protocol.Compute;
import io.camunda.zeebe.util.sched.akka.Protocol.Execute;
import io.camunda.zeebe.util.sched.akka.Protocol.Result;

public final class OperationBehavior extends AbstractBehavior<Protocol.Operation> {

  public OperationBehavior(final ActorContext<Protocol.Operation> context) {
    super(context);
  }

  @Override
  public Receive<Protocol.Operation> createReceive() {
    return newReceiveBuilder()
        .onMessage(Execute.class, this::execute)
        .onMessage(Compute.class, this::compute)
        .build();
  }

  private Behavior<Protocol.Operation> execute(final Execute operation) {
    operation.run();
    return Behaviors.same();
  }

  private <V> Behavior<Protocol.Operation> compute(final Compute<V> operation) {
    final var replyTo = operation.getReplyTo();

    try {
      final var result = operation.call();
      replyTo.tell(StatusReply.success(new Result<>(result)));
    } catch (final Exception e) {
      replyTo.tell(StatusReply.error(e));
    }

    return Behaviors.same();
  }
}
