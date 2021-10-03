package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import io.camunda.zeebe.util.sched.akka.messages.Message;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is the equivalent of {@link io.camunda.zeebe.util.sched.Actor}.
 *
 * <p>There are some notable differences with the starting and closing phases. In the older actor
 * model, an actor would transition from STARTING to STARTED, or CLOSING to CLOSED, if it completed
 * all tasks enqueued during the STARTING/CLOSING phase. Here, we adopt an explicit transition
 * model, where {@link ActorControl#completeStart()} or {@link ActorControl#completeClose()} must
 * be explicitly called to perform the transition.
 *
 * <p>There's a clear issue with the control not being known at construction time, for various
 * reasons, but it means it may be null until the {@link #registerBarrier} is reached. This is
 * terrible, and hopefully we could come up with a better solution.
 */
public abstract class ActorAdapter implements AutoCloseable {
  protected ActorControl actor;

  private final AtomicBoolean registered = new AtomicBoolean();
  private final CompletableFuture<Void> registerBarrier = new CompletableFuture<>();
  private final CompletableFuture<Void> shutdownBarrier = new CompletableFuture<>();
  private final CompletableFuture<Void> startupBarrier = new CompletableFuture<>();

  /**
   * Triggers graceful shutdown of the attached actor, and awaits its completion with a default
   * timeout of 10 seconds.
   *
   * @throws InterruptedException if the thread is interrupted while awaiting shutdown
   * @throws TimeoutException if shutdown takes more than 10 seconds
   * @throws ExecutionException if an error occurs during graceful shutdown
   */
  @Override
  public final void close() throws InterruptedException, TimeoutException, ExecutionException {
    closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  /**
   * Triggers graceful shutdown of the attach actor, returning a future which is completed only when
   * the actor is fully stopped.
   *
   * @return a future which completes when the attached actor is stopped
   */
  public final CompletionStage<Void> closeAsync() {
    return registerBarrier.thenCompose(
        ok -> {
          actor.requestClose();
          return shutdownBarrier;
        });
  }

  /**
   * Does not actually perform any startup logic but allows awaiting for the startup to finish. To
   * be determined if we would want to trigger the startup manually though and not in the
   * constructor.
   *
   * @return a future which is completed after {@link #onActorStarted()} has been called
   *     successfully
   */
  public final CompletionStage<Void> startAsync() {
    return registerBarrier.thenCompose(ok -> startupBarrier);
  }

  /** @return a human readable identifier for this adapter */
  protected String getName() {
    return getClass().getSimpleName();
  }

  /**
   * Called immediately after the adapter has been registered with the actor, and before any other
   * lifecycle methods.
   *
   * <p>This event is more of a notification.
   *
   * <p>Only called once immediately after registration.
   */
  protected void onActorStarting() {}

  /**
   * Called only once after {@link ActorControl#completeStart()} has been called in the starting
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
   * This method is called whenever the actor is closed on failure, allowing actor specific error
   * handling. This should most likely be replaced by a combination of graceful shutdown and error
   * handling on a per actor basis.
   */
  @SuppressWarnings("unused")
  protected void onActorFailed(final Throwable error) {}

  /**
   * Invoked when a {@link io.camunda.zeebe.util.sched.akka.messages.Execute} task throws an error
   * in the STARTED phase. Emulates the previous actor behavior.
   *
   * @param error the error thrown
   */
  protected void handleTaskFailure(final Exception error) {
    actor.getLog().trace("Unhandled failure occurred in STARTED phase", error);
  }

  /**
   * This method is called by the {@link ActorMonitorBehavior} when the associated actor is stopped
   * with an error.
   *
   * @param error the error that caused the actor to fail
   */
  protected final void fail(final Throwable error) {
    registerBarrier.completeExceptionally(error);
    startupBarrier.completeExceptionally(error);
    shutdownBarrier.completeExceptionally(error);
  }

  /**
   * This method is called by the {@link ActorMonitorBehavior} when the associated actor is stopped
   * successfully.
   */
  protected final void terminate() {
    registerBarrier.complete(null);
    startupBarrier.complete(null);
    shutdownBarrier.complete(null);
  }

  /**
   * Called by the attached actor when it transitions to STARTED, after {@link #onActorStarted()}
   * has been called successfully.
   */
  protected final void completeStartup() {
    startupBarrier.complete(null);
  }

  /**
   * Registers a given actor and its system to the adapter, thereby building its {@link
   * ActorControl}.
   *
   * <p>This method is thread safe, and only the first call will actually attach an actor to the
   * adapter; subsequent calls are ignored. This means that any access to the {@link #actor} from
   * outside the synchronization context (e.g. {@link #closeAsync()}) must first wait until the
   * {@link #registerBarrier} is reached before accessing the {@link #actor}, as otherwise it may be
   * null.
   *
   * <p>Any calls from within the synchronization context are guaranteed to have access to it
   * though, or any calls
   *
   * <p>One caveat: not calling this means that most likely nothing else with the actor will work.
   */
  protected final void register(final ActorRef<Message> actor, final ActorSystem<?> system) {
    if (registered.compareAndSet(false, true)) {
      this.actor = new ActorControl(actor, system);
      registerBarrier.complete(null);
    }
  }
}
