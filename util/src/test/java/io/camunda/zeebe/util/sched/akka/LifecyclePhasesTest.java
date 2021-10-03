package io.camunda.zeebe.util.sched.akka;

import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.CLOSED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.CLOSE_REQUESTED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.CLOSING;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.FAILED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.STARTED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.STARTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import io.camunda.zeebe.util.sched.akka.messages.BlockingExecute.TaskControl;
import io.camunda.zeebe.util.sched.akka.util.ActorFailedException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Lifecycle tests adapted from {@link
 * io.camunda.zeebe.util.sched.lifecycle.ActorLifecyclePhasesTest}. Some differences apply when it
 * comes to starting/closing phases.
 */
final class LifecyclePhasesTest {
  private static final ActorTestKit ACTORS = ActorTestKit.create();
  private static final ActorSchedulingService SCHEDULER = new ActorTestKitSchedulingService(ACTORS);

  @AfterAll
  static void afterAll() {
    ACTORS.shutdownTestKit();
  }

  @Test
  void shouldStart() {
    // given
    final var actor = new LifecycleRecordingActor(true);
    SCHEDULER.submitActorAsync(actor);

    // when
    final var startedFuture = actor.startAsync();

    // then
    assertThat(startedFuture).succeedsWithin(Duration.ofSeconds(5));
    assertThat(actor.phases).containsExactly(STARTING, STARTED);
  }

  @Test
  void shouldCloseInStartingState() {
    // given
    final var actor = new LifecycleRecordingActor(false, true);
    SCHEDULER.submitActorAsync(actor);

    // when
    final CompletionStage<Void> closeFuture = actor.closeAsync();

    // then
    assertThat(closeFuture).succeedsWithin(Duration.ofSeconds(5));
    assertThat(actor.phases).containsExactly(STARTING, CLOSE_REQUESTED, CLOSING, CLOSED);
  }

  @Test
  void shouldCloseInStartedState() {
    // given
    final var actor = new LifecycleRecordingActor(true);

    // when
    SCHEDULER.submitActor(actor);
    final var closeFuture = actor.closeAsync();

    // then
    assertThat(closeFuture).succeedsWithin(Duration.ofSeconds(5));
    assertThat(actor.phases).containsExactly(STARTING, STARTED, CLOSE_REQUESTED, CLOSING, CLOSED);
  }

  @Test
  void shouldCloseOnFailureWhileActorStarting() {
    // given
    final var failure = new RuntimeException("foo");
    final var actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarting() {
            super.onActorStarting();
            throw failure;
          }
        };
    SCHEDULER.submitActorAsync(actor);

    // when
    final CompletionStage<Void> startedFuture = actor.startAsync();

    // then
    assertThat(startedFuture)
        .failsWithin(Duration.ofSeconds(5))
        .withThrowableOfType(ExecutionException.class)
        .havingRootCause()
        .isEqualTo(failure);
    assertThat(actor.phases).containsExactly(STARTING);
  }

  @Test
  void shouldCloseOnFailureWhileActorClosing() {
    // given
    final var failure = new RuntimeException("foo");
    final var actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorClosing() {
            super.onActorClosing();
            throw failure;
          }
        };
    SCHEDULER.submitActorAsync(actor);

    // when
    final CompletionStage<Void> closeFuture = actor.closeAsync();

    // then
    assertThat(closeFuture)
        .failsWithin(Duration.ofSeconds(5))
        .withThrowableOfType(ExecutionException.class)
        .havingRootCause()
        .isEqualTo(failure);
    assertThat(actor.phases).containsExactly(STARTING, CLOSE_REQUESTED, CLOSING, CLOSED);
  }

  @Test
  void shouldPropagateFailureWhileActorStartingAndRun() {
    // given
    final var failure = new RuntimeException("foo");
    final var actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarting() {
            super.onActorStarting();
            actor.run(
                () -> {
                  throw failure;
                });
          }
        };
    SCHEDULER.submitActorAsync(actor);

    // when
    final CompletionStage<Void> startedFuture = actor.startAsync();

    // then
    assertThat(startedFuture)
        .failsWithin(Duration.ofSeconds(5))
        .withThrowableOfType(ExecutionException.class)
        .havingRootCause()
        .isEqualTo(failure);
    assertThat(actor.phases).containsExactly(STARTING, CLOSED);
  }

  @Test
  void shouldPropagateFailureWhileActorClosingAndRun() {
    // given
    final var failure = new RuntimeException("foo");
    final var actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorClosing() {
            super.onActorClosing();
            actor.run(
                () -> {
                  throw failure;
                });
          }
        };
    SCHEDULER.submitActorAsync(actor);

    // when
    final CompletionStage<Void> closeFuture = actor.closeAsync();

    // then
    assertThat(closeFuture)
        .failsWithin(Duration.ofSeconds(5))
        .withThrowableOfType(ExecutionException.class)
        .havingRootCause()
        .isEqualTo(failure);
    assertThat(actor.phases).containsExactly(STARTING, CLOSE_REQUESTED, CLOSING, CLOSED);
  }

  @Test
  void shouldDiscardJobsOnFailureWhileActorStarting() {
    // given
    final var failure = new RuntimeException("foo");
    final var isInvoked = new AtomicBoolean();
    final var actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarting() {
            super.onActorStarting();
            actor.run(() -> isInvoked.set(true));
            throw failure;
          }
        };
    SCHEDULER.submitActorAsync(actor);

    // when
    final CompletionStage<Void> startedFuture = actor.startAsync();

    // then
    assertThat(startedFuture)
        .failsWithin(Duration.ofSeconds(5))
        .withThrowableOfType(ExecutionException.class)
        .havingRootCause()
        .isEqualTo(failure);
    assertThat(isInvoked).isFalse();
  }

  @Test
  void shouldNotCloseOnFailureWhileActorStarted() throws InterruptedException {
    // given
    final AtomicReference<Exception> handledFailure = new AtomicReference<>();
    final var doneRunningBarrier = new CountDownLatch(1);
    final var invocations = new AtomicInteger();
    final var failure = new RuntimeException("foo");
    final var actor =
        new LifecycleRecordingActor(true) {
          @Override
          public void onActorStarted() {
            super.onActorStarted();
            actor.runUntilDone(
                () -> {
                  final int inv = invocations.getAndIncrement();

                  if (inv == 0) {
                    throw failure;
                  } else if (inv == 10) {
                    // signal we're done with blocking and we can now close
                    actor.run(doneRunningBarrier::countDown);
                    return TaskControl.DONE;
                  } else {
                    return TaskControl.YIELD;
                  }
                });
          }

          @Override
          public void handleTaskFailure(final Exception error) {
            super.handleTaskFailure(error);
            handledFailure.set(error);
          }
        };

    // when
    SCHEDULER.submitActor(actor);

    // then
    assertThat(doneRunningBarrier.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(handledFailure).hasValue(failure);
    assertThat(actor.phases).containsExactly(STARTING, STARTED);
  }

  @Test
  void shouldActorSpecificHandleException() {
    // given
    final var invocations = new AtomicInteger();
    final var actor =
        new LifecycleRecordingActor(true) {
          @Override
          public void onActorStarted() {
            super.onActorStarted();
            actor.run(
                () -> {
                  throw new RuntimeException("foo");
                });
          }

          @Override
          public void handleTaskFailure(final Exception failure) {
            super.handleTaskFailure(failure);
            invocations.incrementAndGet();
          }
        };

    // when
    SCHEDULER.submitActor(actor);

    // then
    await("until failure is handled").untilAtomic(invocations, IsEqual.equalTo(1));
  }

  @Test
  void shouldHandleFailureWhenExceptionOnFutureContinuation() {
    // given
    final CompletableFuture<Void> future = new CompletableFuture<>();
    final AtomicReference<Throwable> handledFailure = new AtomicReference<>();
    final var failure = new RuntimeException("foo");
    final var actor =
        new LifecycleRecordingActor(true) {
          @Override
          public void onActorStarted() {
            super.onActorStarted();
            actor.runOnCompletion(
                future,
                (v, t) -> {
                  throw failure;
                });
            actor.run(() -> future.complete(null));
          }

          @Override
          public void handleTaskFailure(final Exception failure) {
            super.handleTaskFailure(failure);
            handledFailure.set(failure);
          }
        };

    // when
    SCHEDULER.submitActor(actor);

    // then
    await("until failure is handled").untilAtomic(handledFailure, IsEqual.equalTo(failure));
  }

  @Test
  void shouldNotExecuteNextJobsOnFail() {
    // given
    final var invocations = new AtomicInteger();
    final var actor =
        new LifecycleRecordingActor(true) {
          @Override
          public void onActorStarted() {
            super.onActorStarted();
            actor.fail();
            actor.submit(invocations::incrementAndGet);
          }

          @Override
          public void handleTaskFailure(final Exception failure) {
            super.handleTaskFailure(failure);
            invocations.incrementAndGet();
          }
        };

    // when
    SCHEDULER.submitActor(actor);

    // then - wait until we're fully started before requesting close to avoid flakiness and ensure
    // we failed before close was requested
    assertThat(actor.startAsync()).succeedsWithin(Duration.ofSeconds(5));
    assertThat(actor.closeAsync())
        .failsWithin(Duration.ofSeconds(5))
        .withThrowableOfType(ExecutionException.class)
        .havingRootCause()
        .isInstanceOf(ActorFailedException.class);
    assertThat(invocations).hasValue(0);
    assertThat(actor.phases).containsExactly(STARTING, STARTED, FAILED);
  }
}
