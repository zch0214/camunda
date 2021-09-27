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
import io.camunda.zeebe.util.sched.akka.messages.Message;
import io.camunda.zeebe.util.sched.akka.messages.RegisterAdapter;
import io.camunda.zeebe.util.sched.akka.messages.Start.Started;
import java.util.EnumSet;
import java.util.Set;

/**
 * A class which implements the lifecycle and basic control mechanisms to help us migrate from our
 * old actor scheduler to Akka.
 *
 * <p>The actor uses a basic FSM pattern. Initially, it accepts only {@link RegisterAdapter}
 * messages.
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
 */
public class ActorBehavior extends AbstractBehavior<Message> {
  private static final Set<ActorLifecyclePhase> EXECUTABLE_PHASES =
      EnumSet.of(
          ActorLifecyclePhase.STARTING, ActorLifecyclePhase.STARTED, ActorLifecyclePhase.CLOSING);

  private ActorLifecyclePhase phase;
  private ActorAdapter adapter;

  public ActorBehavior(final ActorContext<Message> context) {
    super(context);
  }

  @Override
  public Receive<Message> createReceive() {
    return newReceiveBuilder().onMessage(RegisterAdapter.class, this::onRegisterAdapter).build();
  }

  private <V> Behavior<Message> onCompute(final Compute<V> message) {
    if (EXECUTABLE_PHASES.contains(phase)) {
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
    if (EXECUTABLE_PHASES.contains(phase)) {
      getContext()
          .getLog()
          .trace(
              "Dropping execute message {} sent in phase {}; these are only accepted in {} phases",
              message,
              phase,
              EXECUTABLE_PHASES);
      return Behaviors.unhandled();
    }

    message.run();
    return Behaviors.same();
  }

  private Behavior<Message> onBlockingExecute(final BlockingExecute message) {
    if (EXECUTABLE_PHASES.contains(phase)) {
      getContext()
          .getLog()
          .trace(
              "Dropping blocking execute message {} sent in phase {}; these are only accepted in {} phases",
              message,
              phase,
              EXECUTABLE_PHASES);
      return Behaviors.unhandled();
    }

    final var taskFlowControl = message.run();
    if (taskFlowControl != BlockingExecute.TaskControl.DONE) {
      getContext().getSelf().tell(message);

      // could be optimized to already know that we're in a blocking execute state, and return
      // Behaviors.same() instead
      return newReceiveBuilder()
          .onMessageEquals(message, () -> onBlockingExecute(message))
          .onSignal(PostStop.class, postStop -> onClosed())
          .build();
    }

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

  private Behavior<Message> onRegisterAdapter(final RegisterAdapter message) {
    // Hard fail in order to propagate that the actor should be closed and recreated
    if (adapter != null) {
      final var errorMessage =
          String.format(
              "Expected to register adapter %s on %s, but %s is already the registered adapter",
              message.getAdapter(), getContext().getSelf(), adapter);
      throw new IllegalStateException(errorMessage);
    }

    adapter = message.getAdapter();
    message.getReplyTo().tell(StatusReply.success(null));

    phase = ActorLifecyclePhase.STARTING;
    adapter.onActorStarting();

    return startingState();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onStarted(final Started message) {
    phase = ActorLifecyclePhase.STARTED;

    if (adapter != null) {
      adapter.onActorStarted();
    }

    return startedState();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onClose(final Close message) {
    phase = ActorLifecyclePhase.CLOSE_REQUESTED;
    if (adapter != null) {
      adapter.onActorCloseRequested();
    }

    // transition to closing by sending ourselves a message; this is useful primarily so we can
    // discard Compute and Execute messages between CLOSE_REQUESTED and CLOSING, keeping the same
    // behavior as we previously had, i.e. "discarding" previously enqueued jobs
    getContext().getSelf().tell(new Closing());

    return closeRequestedState();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onClosing(final Closing message) {
    phase = ActorLifecyclePhase.CLOSING;
    if (adapter != null) {
      adapter.onActorClosing();
    }

    return closingState();
  }

  private Behavior<Message> onClosed() {
    phase = ActorLifecyclePhase.CLOSED;

    if (adapter != null) {
      adapter.onActorClosed();
      adapter.completeShutdown();
      adapter = null;
    }

    return Behaviors.stopped();
  }

  @SuppressWarnings("unused")
  private Behavior<Message> onBlockPhase(final BlockPhase message) {
    getContext().getLog().trace("Blocking lifecycle phase of actor in phase {}", phase);

    return newReceiveBuilder()
        .onMessage(UnblockPhase.class, this::onUnblockPhase)
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
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

  private Receive<Message> startingState() {
    return newReceiveBuilder()
        .onMessage(BlockPhase.class, this::onBlockPhase)
        .onMessage(Started.class, this::onStarted)
        .onMessage(Close.class, this::onClose)
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onMessage(BlockingExecute.class, this::onBlockingExecute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> startedState() {
    return newReceiveBuilder()
        .onMessage(BlockPhase.class, this::onBlockPhase)
        .onMessage(Close.class, this::onClose)
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> closeRequestedState() {
    return newReceiveBuilder()
        .onMessage(Closing.class, this::onClosing)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }

  private Receive<Message> closingState() {
    return newReceiveBuilder()
        .onMessage(BlockPhase.class, this::onBlockPhase)
        .onMessage(Closed.class, close -> onClosed())
        .onMessage(Execute.class, this::onExecute)
        .onMessage(Compute.class, this::onCompute)
        .onSignal(PostStop.class, postStop -> onClosed())
        .build();
  }
}
