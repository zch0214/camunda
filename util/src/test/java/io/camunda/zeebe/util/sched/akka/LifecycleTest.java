package io.camunda.zeebe.util.sched.akka;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.BehaviorTestKit;
import akka.actor.typed.javadsl.Behaviors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

final class LifecycleTest {
  private static final ActorTestKit testKit = ActorTestKit.create();

  @AfterAll
  static void afterAll() {
    testKit.shutdownTestKit();
  }

  @Test
  void shouldTest() {}

  @Test
  void shouldSpawnInStartingPhase() {
    // given
    final var behavior = Behaviors.setup(ActorBehavior::new);

    // when
    final var testKit = BehaviorTestKit.create(behavior);

    // then
  }

  private static final class TestActor extends ActorAdapter {

    public TestActor(final ActorControl control) {
      super(control);
    }
  }
}
