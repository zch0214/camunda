package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.japi.function.Creator;
import akka.pattern.StatusReply;
import io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase;
import io.camunda.zeebe.util.sched.akka.messages.BlockPhase;
import io.camunda.zeebe.util.sched.akka.messages.BlockPhase.UnblockPhase;
import io.camunda.zeebe.util.sched.akka.messages.BlockingExecute;
import io.camunda.zeebe.util.sched.akka.messages.Close;
import io.camunda.zeebe.util.sched.akka.messages.Close.Closed;
import io.camunda.zeebe.util.sched.akka.messages.Close.Closing;
import io.camunda.zeebe.util.sched.akka.messages.Compute;
import io.camunda.zeebe.util.sched.akka.messages.Execute;
import io.camunda.zeebe.util.sched.akka.messages.Fail;
import io.camunda.zeebe.util.sched.akka.messages.Message;
import io.camunda.zeebe.util.sched.akka.messages.Start;
import io.camunda.zeebe.util.sched.akka.messages.Start.Started;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * A class which implements the lifecycle and basic control mechanisms to help us migrate from our
 * old actor scheduler to Akka.
 *
 * <p>The actor uses a basic FSM pattern. Initially, it accepts only {@link
 * io.camunda.zeebe.util.sched.akka.messages.Start} messages.
 *
 * <p>Once it receives one, it will cache the lifecycle, and transition to state starting, notifying
 * the {@link ActorAdapter} instance. In this state, it only accepts {@link Started} messages.
 *
 * <p>Once it receives a {@link Started} message, it will transition to started. In this state, it
 * accepts {@link Compute} and {@link Execute} messages.
 *
 * <p>When it is terminated, it will first terminate all of its children recursively. Once they are
 * all terminated, it will then receive its {@link PostStop} signal, and will call one final time
 * the {@link ActorAdapter#onActorClosed()} for clean up. At this point it's guaranteed not to
 * process any more messages.
 *
 * <p>This is different from the normal Zeebe actor behavior. Let's start with the starting phase.
 *
 * <p>To complete the starting phase, you will need to send a message {@link Started}; otherwise
 * without it, you will be stuck in starting phase.
 *
 * <p>The biggest difference is then when it comes to closing. We try to simulate the old behavior
 * as closely as possible, but there are some differences. For example, if your actor is forcefully
 * closed (for example, its parent dies unexpectedly), then only {@link
 * ActorAdapter#onActorClosed()} will be closed, as there is no way to hook into the closing phase.
 *
 * <p>To gracefully stop an adapter, you can send a {@link Close} message, which will call {@link
 * ActorAdapter#onActorCloseRequested()}, then {@link ActorAdapter#onActorClosing()}. It will also
 * send a {@link Close} message to all of its children.
 *
 * <p>It's possible to block phases by using a {@link BlockPhase} (and converse {@link UnblockPhase}
 * message. This transitions the behavior to a state where it does not accept any messages but the
 * {@link PostStop} signal (in case of being forcefully closed), and the {@link UnblockPhase} to
 * unblock the current phase. It's recommended you avoid blocking phases as this just adds
 * complexity.
 *
 * <p>runUntilDone is implemented by using a {@link BlockingExecute} message. When processing this
 * message, no other messages are then received/handled EXCEPT that exact message instance (see
 * {@link akka.actor.typed.javadsl.ReceiveBuilder#onMessageEquals(Object, Creator)} for more), and
 * the {@link PostStop} signal to guarantee cleanup. One thing to verify is if it should be possible
 * to trigger graceful shutdown - I wasn't sure about that. Maybe we should allow it?
 *
 * <p>There's also another caveat with runUntilDone - previously, it would run in a closed loop
 * blocking both the actor and the thread. While it doesn't do this now (which is good), it will now
 * drop messages in between. So if you do, e.g., runUntilDone(op), closeAsync, it will then produce
 * Execute(op), Close(), Execute(op), Execute(op), ..., and the Close() will be discarded since it
 * was in a `runUntilDone` phase, and we aren't buffering messages. The other option is to block the
 * actor and do a busy loop over the task, but that's...well, that's bad. But maybe we should do
 * that since it more closely emulates the previous behavior?
 *
 * <p>The actor handles the PostStop signal and thus ALWAYS goes into CLOSED state at the end,
 * regardless of whether it failed or not previously.
 */
public class ActorBehavior extends AbstractBehavior<Message> {
  private static final Set<ActorLifecyclePhase> EXECUTABLE_PHASES =
      EnumSet.of(
          ActorLifecyclePhase.STARTING, ActorLifecyclePhase.STARTED, ActorLifecyclePhase.CLOSING);

  private final ActorAdapter adapter;
  private ActorLifecyclePhase phase;

  public ActorBehavior(final ActorContext<Message> context, final ActorAdapter adapter) {
    super(context);
    this.adapter = Objects.requireNonNull(adapter);
  }

  @Override
  public Receive<Message> createReceive() {
    return initialState();
  }

  private <V> Behavior<Message> onCompute(final Compute<V> message) {
    if (!EXECUTABLE_PHASES.contains(phase)) {
      getContext()
          .getLog()
          .trace(
              "Dropping compute message {} sent in phase {}; these are only accepted in {} phases",
              message,
              phase,
              EXECUTABLE_PHASES);
      return Behaviors.unhandled();
    }

    StatusReply<V> reply;
    try {
      reply = StatusReply.success(message.call());
    } catch (final Exception e) {
      reply = StatusReply.error(e);
    }

    message.getReplyTo().tell(reply);
    return Behaviors.same();
  }

  /**
   * Caveat: currently, on error, the actor will die. This is different than our actors, where the
   * actor will log the error and move on to the next message.
   */
  private Behavior<Message> onExecute(final Execute message) {
    if (!EXECUTABLE_PHASES.contains(phase)) {
      getContext()
          .getLog()
          .trace(
              "Dropping execute message {} sent in phase {}; these are only accepted in {} phases",
              message,
              phase,
              EXECUTABLE_PHASES);
      return Behaviors.unhandled();
    }

    try {
      message.run();
    } catch (final Exception e) {
      if (phase == ActorLifecyclePhase.STARTED) {
        adapter.handleTaskFailure(e);
      } else {
        throw e;
      }
    }

    return Behaviors.same();
  }

  private Behavior<Message> onBlockingExecute(final BlockingExecute message) {
    if (!EXECUTABLE_PHASES.contains(phase)) {
      getContext()
          .getLog()
          .trace(
              "Dropping blocking execute message {} sent in phase {}; these are only accepted in {} phases",
              message,
              phase,
              EXECUTABLE_PHASES);
      return Behaviors.unhandled();
    }

    try {
      final var taskFlowControl = message.run();
      if (taskFlowControl == BlockingExecute.TaskControl.DONE) {
        switch (phase) {
          case STARTING:
            return startingState();
          case STARTED:
            return startedState();
          case CLOSING:
            return closingState();
          default:
            throw new IllegalStateException(
                "Should not be accepting blocking execute in a non executable phase " + phase);
        }
      }
    } catch (final Exception e) {
      if (phase == ActorLifecyclePhase.STARTED) {
        adapter.handleTaskFailure(e);
      } else {
        throw e;
      }
    }

    getContext()
        .getLog()
        .trace("Re-executing task {} in a blocking fashion until it's done", message);
    getContext().getSelf().tell(message);

    // could be optimized to already know that we're in a blocking execute state, and return
    // Behaviors.same() instead
    return blockingExecuteState(message);
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onStart(final Start message) {
    getContext().getLog().trace("Transitioning to lifecycle state STARTING");
    phase = ActorLifecyclePhase.STARTING;

    // TODO: evaluate if instead we should share the context. this is must riskier as it exposes
    //       the whole actor, instead of just the reference/system (which are already fine to
    //       share), but it means timers are handled properly for example, and we can pipe futures
    //       to ourselves instead of having two layers of indirection
    adapter.register(getContext().getSelf(), getContext().getSystem());

    adapter.onActorStarting();
    return startingState();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onStarted(final Started message) {
    getContext().getLog().trace("Transitioning to lifecycle state STARTED");
    phase = ActorLifecyclePhase.STARTED;

    adapter.onActorStarted();
    adapter.completeStartup();

    return startedState();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onClose(final Close message) {
    getContext().getLog().trace("Transitioning to lifecycle state CLOSE_REQUESTED");
    phase = ActorLifecyclePhase.CLOSE_REQUESTED;
    adapter.onActorCloseRequested();

    // transition to closing by sending ourselves a message; this is useful primarily so we can
    // discard Compute and Execute messages between CLOSE_REQUESTED and CLOSING, keeping the same
    // behavior as we previously had, i.e. "discarding" previously enqueued jobs
    getContext().getSelf().tell(new Closing());

    return closeRequestedState();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onClosing(final Closing message) {
    getContext().getLog().trace("Transitioning to lifecycle state CLOSING");
    phase = ActorLifecyclePhase.CLOSING;
    adapter.onActorClosing();

    return closingState();
  }

  private Behavior<Message> onClosed() {
    if (phase == ActorLifecyclePhase.CLOSED) {
      getContext()
          .getLog()
          .trace(
              "Skip CLOSED transitioned which was already perform; this can occur due to a PostStop"
                  + " signal after graceful shutdown");
      return Behaviors.stopped();
    }

    getContext().getLog().trace("Transitioning to lifecycle state CLOSED");
    phase = ActorLifecyclePhase.CLOSED;
    adapter.onActorClosed();

    return Behaviors.stopped();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onBlockPhase(final BlockPhase message) {
    getContext().getLog().trace("Blocking lifecycle phase of actor in phase {}", phase);

    return blockedPhaseState();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onUnblockPhase(final UnblockPhase message) {
    getContext().getLog().trace("Unblocking lifecycle phase of actor in phase {}", phase);

    switch (phase) {
      case STARTING:
        return startingState();
      case STARTED:
        return startedState();
      case CLOSING:
        return closingState();
      default:
        throw new IllegalStateException(
            "Expected to block/unblock only in one of the executable phases, but current phase is "
                + phase);
    }
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onFail(final Fail message) {
    phase = ActorLifecyclePhase.FAILED;
    getContext().getLog().trace("Transitioning to lifecycle state FAILED");

    adapter.onActorFailed(message.getError());
    adapter.fail(message.getError());

    // a little weird, but emulates the existing actor lifecycle - failed simply stops, but doesn't
    // close the actor. we could look into whether this makes sense though...
    return Behaviors.ignore();
  }

  private Receive<Message> initialState() {
    return newReceiveBuilder().onMessage(Start.class, this::onStart).build();
  }

  private Receive<Message> blockedPhaseState() {
    return newReceiveBuilder()
        .onMessage(UnblockPhase.class, this::onUnblockPhase)
        .onMessage(Fail.class, this::onFail)
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> blockingExecuteState(final BlockingExecute message) {
    return newReceiveBuilder()
        .onMessageEquals(message, () -> onBlockingExecute(message))
        .onMessage(Fail.class, this::onFail)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> startingState() {
    return newReceiveBuilder()
        .onMessage(BlockPhase.class, this::onBlockPhase)
        .onMessage(Started.class, this::onStarted)
        .onMessage(Close.class, this::onClose)
        .onMessage(BlockingExecute.class, this::onBlockingExecute)
        .onMessage(Fail.class, this::onFail)
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> startedState() {
    return newReceiveBuilder()
        .onMessage(BlockPhase.class, this::onBlockPhase)
        .onMessage(Close.class, this::onClose)
        .onMessage(BlockingExecute.class, this::onBlockingExecute)
        .onMessage(Fail.class, this::onFail)
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> closeRequestedState() {
    return newReceiveBuilder()
        .onMessage(Closing.class, this::onClosing)
        .onMessage(Fail.class, this::onFail)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> closingState() {
    return newReceiveBuilder()
        .onMessage(BlockPhase.class, this::onBlockPhase)
        .onMessage(Closed.class, close -> onClosed())
        .onMessage(Fail.class, this::onFail)
        .onMessage(BlockingExecute.class, this::onBlockingExecute)
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }
}
