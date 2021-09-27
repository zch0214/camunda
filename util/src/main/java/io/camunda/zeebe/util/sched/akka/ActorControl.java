package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.japi.function.Creator;
import io.camunda.zeebe.util.sched.ActorCondition;
import io.camunda.zeebe.util.sched.ScheduledTimer;
import io.camunda.zeebe.util.sched.akka.messages.BlockPhase;
import io.camunda.zeebe.util.sched.akka.messages.BlockPhase.UnblockPhase;
import io.camunda.zeebe.util.sched.akka.messages.BlockingExecute;
import io.camunda.zeebe.util.sched.akka.messages.BlockingExecute.TaskControl;
import io.camunda.zeebe.util.sched.akka.messages.Close;
import io.camunda.zeebe.util.sched.akka.messages.Close.Closed;
import io.camunda.zeebe.util.sched.akka.messages.Compute;
import io.camunda.zeebe.util.sched.akka.messages.Execute;
import io.camunda.zeebe.util.sched.akka.messages.Message;
import io.camunda.zeebe.util.sched.akka.messages.RegisterAdapter;
import io.camunda.zeebe.util.sched.akka.messages.Start.Started;
import io.camunda.zeebe.util.sched.channel.ChannelSubscription;
import io.camunda.zeebe.util.sched.channel.ConsumableChannel;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContextExecutor;

/**
 * Attempts to replicate {@link io.camunda.zeebe.util.sched.ActorControl} behavior but for Akka
 * actors. Meant to be used with an {@link ActorAdapter} and an attached {@link ActorBehavior}.
 */
public final class ActorControl {
  private final ActorRef<Message> actor;
  private final Scheduler scheduler;
  // NOTE: scheduling tasks on this execution context doesn't synchronize them with the actor
  // itself, but simply schedules them on the right thread pool. You still need to schedule them on
  // the actor context via run or call.
  private final ExecutionContextExecutor executionContext;
  private final Logger logger;

  /**
   * Can used from within an actor if need be
   *
   * @param context the actor's context
   */
  public ActorControl(final ActorContext<Message> context) {
    this(context.getSelf(), context.getSystem().scheduler(), context.getExecutionContext());
  }

  /**
   * Constructor for used by a non actor
   *
   * @param actor the actor which acts as the synchronization context
   * @param scheduler the actor's system's scheduler
   * @param executionContext the actor's system's executor
   */
  public ActorControl(
      final ActorRef<Message> actor,
      final Scheduler scheduler,
      final ExecutionContextExecutor executionContext) {
    this.actor = actor;
    this.scheduler = scheduler;
    this.executionContext = executionContext;

    logger = LoggerFactory.getLogger(actor.path().toString());
  }

  /** @return the attached actor reference */
  public ActorRef<Message> getActor() {
    return actor;
  }

  /**
   * Executes the given callback within the actor's execution context. This ensures serialization
   * and thread safety of the callback executions. Any {@link #call(Callable)}, {@link
   * #run(Runnable)}, etc., operations are serialized amongst themselves and order is preserved.
   *
   * <p>Note that these will be discarded unless the actor is in its lifecycle phase of STARTING,
   * STARTED, or CLOSING.
   *
   * @param operation the operation to execute
   * @param <V> the type of the result
   * @return a future which is completed with the result
   */
  public <V> CompletionStage<V> call(final Callable<V> operation) {
    return AskPattern.askWithStatus(
        actor, replyTo -> new Compute<>(replyTo, operation), Duration.ofSeconds(5), scheduler);
  }

  /**
   * Executes the callback within the registered actor's context. Can only be executed in STARTING,
   * STARTED, and CLOSING phases.
   *
   * <p>As with {@link #call(Callable)}, be careful of the what your {@link Runnable} closes over,
   * and make sure it doesn't close over and/or mutate something that's not owned by this actor.
   *
   * @param operation the callback to execute
   */
  @SuppressWarnings("java:S1905")
  public void run(final Runnable operation) {
    actor.tell((Execute) operation::run);
  }

  /**
   * A slightly different implementation of run until done. The task will be run - each time as a
   * single message - until calling {@link Supplier#get()} on it returns {@link TaskControl#DONE}.
   * If it returns anything else, then it's enqueued again. All other messages (except the {@link
   * akka.actor.typed.PostStop} signal are not processed - even other {@link
   * io.camunda.zeebe.util.sched.akka.messages.BlockingExecute} or {@link Execute}. Only the very
   * specific instance will be processed.
   *
   * <p>The implementation is in {@link ActorBehavior} and makes use of {@link
   * akka.actor.typed.javadsl.ReceiveBuilder#onMessageEquals(Object, Creator)}.
   *
   * @param task the task to execute
   */
  public void runUntilDone(final Supplier<TaskControl> task) {
    actor.tell(new BlockingExecute(task));
  }

  /**
   * Executes the given callback when the future completes. The callback is executed within the
   * actor's synchronization context. There is no time out here - if you need it, you should use
   * either {@link akka.pattern.Patterns#after(Duration, akka.actor.Scheduler, ExecutionContext,
   * CompletionStage)} and not this method directly.
   *
   * @param future the future to wait on
   * @param operation the callback to execute on completion
   * @param <T> the type of result
   */
  @SuppressWarnings("java:S1905")
  public <T> void runOnCompletion(
      final CompletionStage<T> future, final BiConsumer<T, Throwable> operation) {
    future.whenCompleteAsync(
        (result, error) -> actor.tell((Execute) () -> operation.accept(result, error)),
        executionContext);
  }

  /**
   * Runs the given callback when the future completes (successfully or otherwise), but first
   * blocking the current lifecycle phase such that no other transition is possible. When blocked,
   * the actor will still accept {@link Compute} and {@link Execute} messages, as it's only possible
   * to block the current phase when in STARTING, STARTED, or CLOSING phase (which all accept these
   * messages).
   *
   * <p>After the operation is ran, the phase is unblocked, and further lifecycle messages can be
   * processed.
   *
   * @param future the future to wait on
   * @param operation the operation to execute once the future is complete
   * @param <T> the type of result to accept
   */
  @SuppressWarnings("java:S1905")
  public <T> void runOnCompletionBlockingPhase(
      final CompletionStage<T> future, final BiConsumer<T, Throwable> operation) {
    actor.tell(new BlockPhase());
    runOnCompletion(
        future,
        (result, error) -> {
          actor.tell(new UnblockPhase());
          operation.accept(result, error);
        });
  }

  /**
   * Runs a task after a delay has elapsed. The task is ran within the actor's context.
   *
   * <p>Note that this timer can be quite inaccurate, as it will enqueue the task in the actor's
   * inbox once the delay has elapsed. If there are already many messages enqueued, it may then
   * accrue even more delay. This is unfortunately unavoidable right now, as we don't have access to
   * the real actor.
   *
   * <p>Cancellation will still cancel the task, even if it was enqueued. This incurs a small
   * penalty as it has to check an atomic boolean, which isn't great - but we don't use timers all
   * that much, so it should be fine.
   *
   * @param delay the delay to wait for before the task is executed
   * @param runnable the task to execute once the delay has elapsed
   * @return a timer which can be cancelled
   */
  public ScheduledTimer runDelayed(final Duration delay, final Runnable runnable) {
    final var task = new CancellableTask(runnable);
    final var cancellable = scheduler.scheduleOnce(delay, () -> run(task), executionContext);

    return () -> {
      task.cancel();
      cancellable.cancel();
    };
  }

  /**
   * Runs a task at a regular interval. The task is ran within the actor's context, and only in the
   * STARTING, STARTED, and CLOSING phase.
   *
   * <p>Note that this timer can be quite inaccurate, as it will enqueue the task in the actor's
   * inbox once the delay has elapsed. If there are already many messages enqueued, it may then
   * accrue even more delay. This is unfortunately unavoidable right now, as we don't have access to
   * the real actor. As such, it's better relatively speaking to use {@link #runDelayed(Duration,
   * Runnable)}, and enqueue the next task once that's done to avoid the risk of enqueuing many
   * tasks and never processing them.
   *
   * <p>Cancellation will still cancel the task, even if it was enqueued. This incurs a small
   * penalty as it has to check an atomic boolean, which isn't great - but we don't use timers all
   * that much, so it should be fine.
   *
   * @param delay the delay to wait for before the task is executed
   * @param runnable the task to execute once the delay has elapsed
   * @return a timer which can be cancelled
   */
  public ScheduledTimer runAtFixedRate(final Duration delay, final Runnable runnable) {
    final var task = new CancellableTask(runnable);
    final var cancellable =
        scheduler.scheduleAtFixedRate(Duration.ZERO, delay, () -> run(task), executionContext);

    return () -> {
      task.cancel();
      cancellable.cancel();
    };
  }

  /**
   * Registers the attached actor as a consumer of this channel. The given task will be called every
   * time there is new data to consume on the channel. The task runs in the attached actor context,
   * and only in the actor STARTING, STARTED, and CLOSING phases.
   *
   * <p>If cancelled, even if the task was already enqueued in the actor's inbox, the actor will
   * process the message but the task will not run.
   *
   * @param channel the channel register a consumer onto
   * @param runnable the task to run whenever new data can be consumed
   * @return a subscription which can be cancelled
   */
  public ChannelSubscription consume(final ConsumableChannel channel, final Runnable runnable) {
    final var isCancelled = new AtomicBoolean();
    final var task = new CancellableTask(runnable, isCancelled);
    final var subscription =
        new ChannelSubscriptionImpl(channel, new CancellableTask(() -> run(task), isCancelled));
    channel.registerConsumer(subscription);

    return subscription;
  }

  /**
   * Creates a condition on the attached actor, with a task that is triggered whenever {@link
   * ActorCondition#signal()} is called on the returned condition. The task runs in the attached
   * actor context, and only in the actor STARTING, STARTED, and CLOSING phases.
   *
   * <p>When cancelled, the task will never run, even if {@link ActorCondition#signal()} is called.
   *
   * <p>If cancelled, even if the task was already enqueued in the actor's inbox, the actor will
   * process the message but the task will not run.
   *
   * @param conditionName the name of the condition for debugging purposes
   * @param conditionAction the task to run whenever the condition is signaled
   * @return a condition
   */
  public ActorCondition onCondition(final String conditionName, final Runnable conditionAction) {
    final var isCancelled = new AtomicBoolean();
    final var task = new CancellableTask(conditionAction, isCancelled);
    return new ActorConditionImpl(conditionName, new CancellableTask(() -> run(task), isCancelled));
  }

  /**
   * Notifies the actor that the STARTING phase is finished, thereby triggering the STARTED phase.
   * This is useful for asynchronous startup phases. This MUST be called during the starting phase,
   * otherwise the actor will remain stuck there.
   */
  public void finishStarting() {
    actor.tell(new Started());
  }

  /**
   * Notifies the actor that it is closing, but transitions to CLOSE_REQUESTED phase. In this phase,
   * {@link Compute} and {@link Execute} messages are ignored and discarded. Only the {@link
   * io.camunda.zeebe.util.sched.akka.messages.Close.Closing} message is accepted to transition to
   * CLOSING phase.
   */
  public void requestClose() {
    actor.tell(new Close());
  }

  /**
   * Notifies the actor that the CLOSING phase is finished, thereby triggering the CLOSED phase.
   * This is useful for asynchronous shutdown phases. This MUST be called during the closing phase,
   * otherwise the actor will remain stuck there.
   */
  public void finishClosing() {
    actor.tell(new Closed());
  }

  /**
   * Registers the given actor adapter to the wrapped behavior. This can only be done once at the
   * beginning. If it fails, the whole thing is now unusable as we will be missing lifecycle events.
   *
   * @param adapter the adapter to register to the wrapped behavior
   */
  public void registerAdapter(final ActorAdapter adapter) {
    final CompletionStage<Void> registered =
        AskPattern.askWithStatus(
            actor,
            replyTo -> new RegisterAdapter(replyTo, adapter),
            Duration.ofSeconds(5),
            scheduler);

    registered.whenCompleteAsync(
        (ok, error) -> onAdapterRegistered(adapter, error), executionContext);
  }

  /**
   * Simulates what on an actor you would get from calling {@link ActorContext#getLog()}
   *
   * @return a logger with the actor path as name
   */
  public Logger getLog() {
    return logger;
  }

  // NOTE: this is not called from within the actor context!
  private void onAdapterRegistered(final ActorAdapter adapter, final Throwable error) {
    if (error != null) {
      getLog().warn("Failed to register adapter {} on actor {}", adapter, actor, error);
    } else {
      getLog().trace("Registered adapter {} on actor {}", adapter, actor);
    }
  }
}
