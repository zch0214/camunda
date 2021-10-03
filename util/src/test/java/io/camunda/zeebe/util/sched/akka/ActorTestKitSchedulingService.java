package io.camunda.zeebe.util.sched.akka;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import io.camunda.zeebe.util.sched.akka.messages.Message;
import java.util.concurrent.CompletionStage;

/**
 * An implementation of a scheduling service which wraps the {@link ActorTestKit} for testing. By
 * default, it will spawn anonymous actors such that you can easily use the test kit as a static
 * rule/extension.
 */
public final class ActorTestKitSchedulingService implements ActorSchedulingService {
  private final ActorTestKit testKit;

  public ActorTestKitSchedulingService(final ActorTestKit testKit) {
    this.testKit = testKit;
  }

  @Override
  public CompletionStage<Void> submitActorAsync(final ActorAdapter adapter) {
    final Behavior<Message> behavior =
        Behaviors.setup(context -> ActorMonitorBehavior.forAdapter(context, adapter));
    testKit.spawn(behavior);

    return adapter.startAsync();
  }
}
