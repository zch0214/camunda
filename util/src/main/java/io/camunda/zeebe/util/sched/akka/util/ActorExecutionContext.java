package io.camunda.zeebe.util.sched.akka.util;

import io.camunda.zeebe.util.sched.akka.ActorControl;
import java.util.Objects;
import scala.concurrent.ExecutionContextExecutor;

public class ActorExecutionContext implements ExecutionContextExecutor {
  private final ActorControl control;
  private final ExecutionContextExecutor executor;

  public ActorExecutionContext(
      final ActorControl control, final ExecutionContextExecutor executor) {
    this.control = Objects.requireNonNull(control);
    this.executor = Objects.requireNonNull(executor);
  }

  @Override
  public void execute(final Runnable runnable) {
    executor.execute(() -> control.run(runnable));
  }

  @Override
  public void reportFailure(final Throwable cause) {
    executor.reportFailure(cause);
  }
}
