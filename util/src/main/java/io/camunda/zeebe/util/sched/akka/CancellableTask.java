package io.camunda.zeebe.util.sched.akka;

import akka.actor.Cancellable;
import java.util.concurrent.atomic.AtomicBoolean;

/** A task which can be cancelled in a thread safe way. */
public final class CancellableTask implements Runnable, Cancellable {
  private final Runnable task;
  private final AtomicBoolean isCancelled;

  public CancellableTask(final Runnable task) {
    this(task, new AtomicBoolean());
  }

  public CancellableTask(final Runnable task, final AtomicBoolean isCancelled) {
    this.task = task;
    this.isCancelled = isCancelled;
  }

  @Override
  public boolean cancel() {
    isCancelled.set(true);
    return true;
  }

  @Override
  public boolean isCancelled() {
    return isCancelled.get();
  }

  @Override
  public void run() {
    if (!isCancelled()) {
      task.run();
    }
  }
}
