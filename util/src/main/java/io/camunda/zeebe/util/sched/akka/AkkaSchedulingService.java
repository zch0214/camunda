package io.camunda.zeebe.util.sched.akka;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import io.camunda.zeebe.util.sched.akka.messages.Message;
import java.util.concurrent.CompletionStage;

/**
 * An implementation of {@link ActorSchedulingService} which takes in an existing actor context and
 * submits the given adapters as child actors.
 *
 * <p>These actors are {@link ActorMonitorBehavior}, which spawn their own child actor representing
 * the actor's synchronization context. The monitor is purely there to handle failure/termination
 * events, which cannot be distinguished by the child actor itself.
 */
public final class AkkaSchedulingService implements ActorSchedulingService {
  private final ActorContext<Message> context;

  public AkkaSchedulingService(final ActorContext<Message> context) {
    this.context = context;
  }

  @Override
  public CompletionStage<Void> submitActorAsync(final ActorAdapter adapter) {
    final Behavior<Message> behavior =
        Behaviors.setup(childContext -> ActorMonitorBehavior.forAdapter(childContext, adapter));
    context.spawn(behavior, adapter.getName());

    return adapter.startAsync();
  }
}
