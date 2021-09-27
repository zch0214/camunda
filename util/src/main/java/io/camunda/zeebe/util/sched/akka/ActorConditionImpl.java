package io.camunda.zeebe.util.sched.akka;

import io.camunda.zeebe.util.sched.ActorCondition;

/**
 * The default {@link ActorCondition} implementation which can be cancelled, forwarding the
 * cancellation to the wrapped {@link CancellableTask}.
 */
public final class ActorConditionImpl implements ActorCondition {
  private final String name;
  private final CancellableTask task;

  public ActorConditionImpl(final String name, final CancellableTask task) {
    this.task = task;
    this.name = name;
  }

  @Override
  public void signal() {
    task.run();
  }

  @Override
  public void cancel() {
    task.cancel();
  }

  @Override
  public String toString() {
    return "AkkaCondition{" + "name='" + name + '\'' + '}';
  }
}
