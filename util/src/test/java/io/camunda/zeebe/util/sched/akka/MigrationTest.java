package io.camunda.zeebe.util.sched.akka;

import static org.assertj.core.api.Assertions.assertThat;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.javadsl.Behaviors;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

final class MigrationTest {
  private static final ActorTestKit KIT = ActorTestKit.create("testSystem");

  @AfterAll
  static void afterAll() {
    KIT.shutdownTestKit();
  }

  @Test
  void shouldImmediatelyBeStarting() {
    // given
    final var testRef = KIT.spawn(Behaviors.setup(ActorBehavior::new));
    final var control =
        new ActorControl(testRef, KIT.system().scheduler(), KIT.system().executionContext());

    // when
    final var testActor = new TestActor(control);

    // then
    assertThat(testActor.wasActorStarting()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(true);
    assertThat(testActor.wasActorStarted()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(false);
    assertThat(testActor.wasActorCloseRequested())
        .succeedsWithin(Duration.ofSeconds(1))
        .isEqualTo(false);
    assertThat(testActor.wasActorClosing()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(false);
    assertThat(testActor.wasActorClosed()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(false);
  }

  @Test
  void shouldBeStarted() {
    // given
    final var testRef = KIT.spawn(Behaviors.setup(ActorBehavior::new));
    final var control =
        new ActorControl(testRef, KIT.system().scheduler(), KIT.system().executionContext());
    final var testActor = new TestActor(control);

    // when
    control.finishStarting();

    // then
    assertThat(testActor.wasActorStarted()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(true);
    assertThat(testActor.wasActorCloseRequested())
        .succeedsWithin(Duration.ofSeconds(1))
        .isEqualTo(false);
    assertThat(testActor.wasActorClosing()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(false);
    assertThat(testActor.wasActorClosed()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(false);
  }

  @Test
  void shouldClose() {
    // given
    final var testRef = KIT.spawn(Behaviors.setup(ActorBehavior::new));
    final var control =
        new ActorControl(testRef, KIT.system().scheduler(), KIT.system().executionContext());
    final var testActor = new TestActor(control);

    // when
    control.requestClose();

    // then
    assertThat(testActor.wasActorCloseRequested())
        .succeedsWithin(Duration.ofSeconds(1))
        .isEqualTo(true);
    assertThat(testActor.wasActorClosing()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(true);
    assertThat(testActor.wasActorClosed()).succeedsWithin(Duration.ofSeconds(1)).isEqualTo(false);
  }

  @Test
  void shouldCloseGracefully() {
    // given
    final var testRef = KIT.spawn(Behaviors.setup(ActorBehavior::new));
    final var control =
        new ActorControl(testRef, KIT.system().scheduler(), KIT.system().executionContext());
    final var testActor = new TestActor(control);

    // when
    control.requestClose();
    control.finishClosing();

    // then
    assertThat(testActor.wasActorCloseRequested).isTrue();
    assertThat(testActor.wasActorClosing).isTrue();
    assertThat(testActor.wasActorClosed).isTrue();
  }

  @Test
  void shouldCloseForcefully() {
    // given
    final var testRef = KIT.spawn(Behaviors.setup(ActorBehavior::new));
    final var control =
        new ActorControl(testRef, KIT.system().scheduler(), KIT.system().executionContext());
    final var testActor = new TestActor(control);

    // when
    KIT.stop(testRef);

    // then
    assertThat(testActor.wasActorCloseRequested).isFalse();
    assertThat(testActor.wasActorClosing).isFalse();
    assertThat(testActor.wasActorClosed).isTrue();
  }

  private static final class TestActor extends ActorAdapter {
    private volatile boolean wasActorStarting;
    private volatile boolean wasActorStarted;
    private volatile boolean wasActorCloseRequested;
    private volatile boolean wasActorClosing;
    private volatile boolean wasActorClosed;

    public TestActor(final ActorControl control) {
      super(control);
    }

    public CompletionStage<Boolean> wasActorStarting() {
      return control.call(() -> wasActorStarting);
    }

    public CompletionStage<Boolean> wasActorStarted() {
      return control.call(() -> wasActorStarted);
    }

    public CompletionStage<Boolean> wasActorCloseRequested() {
      return control.call(() -> wasActorCloseRequested);
    }

    public CompletionStage<Boolean> wasActorClosing() {
      return control.call(() -> wasActorClosing);
    }

    public CompletionStage<Boolean> wasActorClosed() {
      return control.call(() -> wasActorClosed);
    }

    @Override
    protected void onActorStarting() {
      wasActorStarting = true;
    }

    @Override
    protected void onActorStarted() {
      wasActorStarted = true;
    }

    @Override
    protected void onActorCloseRequested() {
      wasActorCloseRequested = true;
    }

    @Override
    protected void onActorClosing() {
      wasActorClosing = true;
    }

    @Override
    protected void onActorClosed() {
      wasActorClosed = true;
    }
  }
}
