package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import io.camunda.zeebe.util.sched.akka.Protocol.Callback;
import io.camunda.zeebe.util.sched.akka.Protocol.Operation;
import io.camunda.zeebe.util.sched.akka.Protocol.Result;
import io.camunda.zeebe.util.sched.akka.Protocol.SerializableCallable;
import io.camunda.zeebe.util.sched.akka.Protocol.SerializableRunnable;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

public final class AkkaControl {
  private final ActorContext<Operation> context;

  public AkkaControl(final ActorContext<Protocol.Operation> context) {
    this.context = context;
  }

  public <V> CompletionStage<V> call(final SerializableCallable<V> operation) {
    final CompletionStage<Result<V>> result =
        AskPattern.ask(
            context.getSelf(),
            replyTo -> new Callback<>(operation, replyTo),
            Duration.ofSeconds(5),
            context.getSystem().scheduler());

    return result.thenApply(Result::getValue);
  }

  public void run(final Runnable action) {
    context.getSelf().tell((SerializableRunnable) action::run);
  }
}
