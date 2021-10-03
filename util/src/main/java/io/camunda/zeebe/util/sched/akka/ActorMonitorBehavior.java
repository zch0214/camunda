package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.ChildFailed;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.japi.function.Function;
import io.camunda.zeebe.util.sched.akka.messages.Message;
import io.camunda.zeebe.util.sched.akka.messages.Start;

/**
 * The root behavior for Zeebe actors which emulate the old actor scheduler behavior. Its only
 * responsibility is to watch the lifecycle actor ({@link ActorBehavior}) and signal the adapter if
 * that actor is stopped exceptionally (via {@link ActorAdapter#fail(Throwable)}) or successfully
 * (via {@link ActorAdapter#terminate()}.
 *
 * <p>Its necessary to have this parent-child relationship as otherwise there is no way for the
 * lifecycle actor itself to know if it fails, and as such the adapter cannot know either.
 *
 * <p>One caveat with this is that since the behavior construction is deferred, calling {@link
 * #forAdapter(ActorContext, ActorAdapter)} doesn't immediately register the actor with the adapter
 * (via {@link ActorAdapter#register(ActorRef, ActorSystem)}. Registering the lifecycle actor and
 * its system with the adapter is thus an asynchronous process. See {@link
 * ActorAdapter#register(ActorRef, ActorSystem)} for more.
 */
public final class ActorMonitorBehavior extends AbstractBehavior<Message> {
  private final ActorAdapter adapter;

  ActorMonitorBehavior(final ActorContext<Message> context, final ActorAdapter adapter) {
    super(context);
    this.adapter = adapter;
  }

  /**
   * Returns a new {@link ActorMonitorBehavior} which can be used as a factory method to
   * start/restart an emulated Zeebe actor. It will create a new child actor called lifecycle, and
   * will watch this actor, forwarding its fail and terminate signal to the given adapter.
   *
   * <p>Note that the expected usage is with {@link Behaviors#setup(Function)}, e.g.
   *
   * <pre>{@code
   * final Behavior<Message> behavior =
   *  Behaviors.setup(childContext -> ActorMonitorBehavior.forAdapter(childContext, adapter));
   * context.spawn(behavior, "myZeebeActor");
   * }</pre>
   *
   * @param context the context of the actor
   * @param adapter the adapter with which the lifecycle actor will be registered
   * @return a new {@link ActorMonitorBehavior}
   */
  public static ActorMonitorBehavior forAdapter(
      final ActorContext<Message> context, final ActorAdapter adapter) {
    final Behavior<Message> behavior =
        Behaviors.setup(childContext -> new ActorBehavior(childContext, adapter));
    final var lifecycleActor = context.spawn(behavior, "lifecycle");
    // the parent must explicitly watch the child actor to receive ChildFailed and Terminated
    // signals
    context.watch(lifecycleActor);

    lifecycleActor.tell(new Start());
    return new ActorMonitorBehavior(context, adapter);
  }

  @Override
  public Receive<Message> createReceive() {
    return newReceiveBuilder()
        // the ordering is important since ChildFailed extends Terminated
        .onSignal(ChildFailed.class, this::onChildFailed)
        .onSignal(Terminated.class, this::onTerminated)
        .build();
  }

  private Behavior<Message> onChildFailed(final ChildFailed signal) {
    adapter.fail(signal.cause());
    return Behaviors.stopped();
  }

  private Behavior<Message> onTerminated(final Terminated signal) {
    adapter.terminate();
    return Behaviors.stopped();
  }
}
