package io.camunda.zeebe.util.sched.akka;

import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.CLOSED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.CLOSE_REQUESTED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.CLOSING;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.FAILED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.STARTED;
import static io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase.STARTING;
import static org.mockito.Mockito.mock;

import io.camunda.zeebe.util.sched.ActorTask.ActorLifecyclePhase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

/**
 * An actor which records its own lifecycle phase transitions, in order, and exposes them in a
 * thread safe way.
 *
 * <p>It can be configured to autocomplete the starting and/or closing phase depending on the tests.
 * By default, it will not autocomplete either phase.
 */
class LifecycleRecordingActor extends ActorAdapter {
  // use a thread safe variant purely for testing purposes
  public final List<ActorLifecyclePhase> phases = Collections.synchronizedList(new ArrayList<>());

  private final boolean autoCompleteStart;
  private final boolean autoCompleteClose;

  public LifecycleRecordingActor() {
    this(false);
  }

  public LifecycleRecordingActor(final boolean autoComplete) {
    this(autoComplete, autoComplete);
  }

  public LifecycleRecordingActor(final boolean autoCompleteStart, final boolean autoCompleteClose) {
    this.autoCompleteStart = autoCompleteStart;
    this.autoCompleteClose = autoCompleteClose;
  }

  @Override
  protected String getName() {
    return "lifecycleRecorder";
  }

  @Override
  public void onActorStarting() {
    super.onActorStarting();
    phases.add(STARTING);

    if (autoCompleteStart) {
      actor.completeStart();
    }
  }

  @Override
  public void onActorStarted() {
    super.onActorStarted();
    phases.add(STARTED);
  }

  @Override
  public void onActorCloseRequested() {
    super.onActorCloseRequested();
    phases.add(CLOSE_REQUESTED);
  }

  @Override
  public void onActorClosing() {
    super.onActorClosing();
    phases.add(CLOSING);

    if (autoCompleteClose) {
      actor.completeClose();
    }
  }

  @Override
  public void onActorClosed() {
    super.onActorClosed();
    phases.add(CLOSED);
  }

  @Override
  public void onActorFailed(final Throwable error) {
    super.onActorFailed(error);
    phases.add(FAILED);
  }

  protected void blockPhase() {
    blockPhase(new CompletableFuture<>(), mock(BiConsumer.class));
  }

  protected void blockPhase(final CompletionStage<Void> future) {
    blockPhase(future, mock(BiConsumer.class));
  }

  @SuppressWarnings("unchecked")
  protected void blockPhase(
      final CompletionStage<Void> future, final BiConsumer<Void, Throwable> consumer) {
    actor.runOnCompletionBlockingCurrentPhase(future, consumer);
  }

  @SuppressWarnings("unchecked")
  protected void runOnCompletion() {
    actor.runOnCompletion(new CompletableFuture<>(), mock(BiConsumer.class));
  }

  @SuppressWarnings("unchecked")
  protected void runOnCompletion(final CompletionStage<Void> future) {
    actor.runOnCompletion(future, mock(BiConsumer.class));
  }

  public ActorControl control() {
    return actor;
  }
}
