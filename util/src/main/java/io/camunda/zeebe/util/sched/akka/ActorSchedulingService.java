package io.camunda.zeebe.util.sched.akka;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Homologue for {@link io.camunda.zeebe.util.sched.ActorSchedulingService} using {@link
 * CompletionStage} instead of {@link io.camunda.zeebe.util.sched.future.ActorFuture}.
 */
public interface ActorSchedulingService {

  /**
   * Submits the adapter as an "actor" asynchronously. This should setup the synchronization context
   * and link it with the adapter, as well as kickstart the lifecycle process by transitioning to
   * STARTING.
   *
   * <p>The returned future is guaranteed to be completed. When successful, it will be completed
   * after the adapter's {@link ActorAdapter#onActorStarted()} method was called. When failed, it
   * will be completed exceptionally with the error.
   *
   * @param adapter the adapter to submit
   * @return a future which is completed when the adapter is started
   */
  CompletionStage<Void> submitActorAsync(final ActorAdapter adapter);

  /**
   * A convenience, synchronous equivalent of {@link #submitActorAsync(ActorAdapter)}, mostly useful
   * for tests or cases where we simply do not care to block. May be removed in production code to
   * discourage usage.
   *
   * @param adapter the adapter to submit
   */
  default void submitActor(final ActorAdapter adapter) {
    submitActorAsync(adapter).toCompletableFuture().orTimeout(15, TimeUnit.SECONDS).join();
  }
}
