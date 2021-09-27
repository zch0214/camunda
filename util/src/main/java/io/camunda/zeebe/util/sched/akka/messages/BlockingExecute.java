package io.camunda.zeebe.util.sched.akka.messages;

import java.util.function.Supplier;

public final class BlockingExecute implements Message {
  private final Supplier<TaskControl> task;

  public BlockingExecute(final Supplier<TaskControl> task) {
    this.task = task;
  }

  public TaskControl run() {
    return task.get();
  }

  public enum TaskControl {
    YIELD,
    DONE;
  }
}
