package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.AskPattern;
import io.camunda.zeebe.util.sched.akka.Protocol.Compute;
import io.camunda.zeebe.util.sched.akka.Protocol.Execute;
import io.camunda.zeebe.util.sched.akka.Protocol.Operation;
import io.camunda.zeebe.util.sched.akka.Protocol.Result;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

// could mostly implement concurrency control but can't since that one expects to deal with
// ActorFuture
public final class AkkaControl {
  private final ActorRef<Operation> actor;
  private final Scheduler scheduler;

  public AkkaControl(final ActorRef<Operation> actor, final Scheduler scheduler) {
    this.actor = actor;
    this.scheduler = scheduler;
  }

  public <V> CompletionStage<V> call(final Callable<V> operation) {
    final CompletionStage<Result<V>> result =
        AskPattern.askWithStatus(
            actor, replyTo -> new Compute<>(operation, replyTo), Duration.ofSeconds(5), scheduler);

    return result.thenApply(Result::getValue);
  }

  public void run(final Runnable operation) {
    actor.tell((Execute) operation::run);
  }

  public <T> void runOnCompletion(
      final CompletionStage<T> future, final BiConsumer<T, Throwable> operation) {
    future.whenComplete((result, error) -> run(() -> operation.accept(result, error)));
  }
}
