package io.camunda.zeebe.util.sched.akka.messages;

import java.util.Objects;

public final class Fail implements Message {
  private final Throwable error;

  public Fail(final Throwable error) {
    this.error = Objects.requireNonNull(error);
  }

  public Throwable getError() {
    return error;
  }
}
