package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.ActorRef;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is the equivalent of {@link io.camunda.zeebe.util.sched.Actor}.
 *
 * <p>Some issues:
 *
 * <ul>
 *   <li>There's a kind of cyclic dependency between the {@link ActorAdapter} and the {@link
 *       ActorControl}. The adapter doesn't really work without being registered, and uses the
 *       control to register itself. Error handling is poor with registration right now, so it would
 *       be nice if somehow you could enforce that it was registered beforehand.
 * </ul>
 */
public abstract class ActorAdapter implements AutoCloseable {
  protected final ActorControl control;
  private final CompletableFuture<Void> shutdownBarrier = new CompletableFuture<>();

  protected ActorAdapter(final ActorControl control) {
    this.control = control;
    control.registerAdapter(this);
  }

  /**
   * Called immediately after the adapter has been registered with the actor, and before any other
   * lifecycle methods. In this phase, you cannot use the control <i>except</i> the {@link
   * ActorControl#finishStarting()} method to transition to STARTED.
   *
   * <p>This event is more of a notification.
   *
   * <p>Only called once immediately after registration.
   */
  protected void onActorStarting() {}

  /**
   * Called only once after {@link ActorControl#finishStarting()} has been called in the starting
   * phase. At this stage, you can now use all {@link ActorControl} methods without any issues. This
   * is the normal place to do your setup.
   */
  protected void onActorStarted() {}

  /**
   * Called if the actor is in the started phase for graceful shutdown, i.e. shutdown triggered
   * manually by a message. If the actor is forcefully closed via {@link
   * akka.actor.typed.javadsl.ActorContext#stop(ActorRef)}, this method is not called.
   */
  protected void onActorCloseRequested() {}

  /**
   * Called immediately after {@link #onActorCloseRequested()} during graceful shutdown. If the
   * actor is forcefully closed via {@link akka.actor.typed.javadsl.ActorContext#stop(ActorRef)},
   * this method is not called.
   */
  protected void onActorClosing() {}

  /**
   * This method is always called, and is always the last lifecycle method to be called, iff the
   * adapter was registered on the actor.
   *
   * <p>This is the place to do any teardown such as closing resources and so on.
   *
   * <p>This method is called during graceful shutdown AND when the actor is closed via {@link
   * akka.actor.typed.javadsl.ActorContext#stop(ActorRef)}, but is guaranteed to be called only
   * once.
   */
  protected void onActorClosed() {}

  /**
   * Called by the attached actor when it finishes, whether gracefully or not. Used to allow for a
   * blocking {@link #close()} implementation.
   */
  protected final void completeShutdown() {
    shutdownBarrier.complete(null);
  }

  /**
   * Triggers graceful shutdown of the attached actor, and awaits its completion with a default
   * timeout of 10 seconds.
   *
   * @throws InterruptedException if the thread is interrupted while awaiting shutdown
   * @throws TimeoutException if shutdown takes more than 10 seconds
   * @throws ExecutionException if an error occurs during graceful shutdown
   */
  @Override
  public void close() throws InterruptedException, TimeoutException, ExecutionException {
    closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  /**
   * Triggers graceful shutdown of the attach actor, returning a future which is completed only when
   * the actor is fully stopped.
   *
   * @return a future which completes when the attached actor is stopped
   */
  public CompletionStage<Void> closeAsync() {
    control.requestClose();
    return shutdownBarrier;
  }
}
