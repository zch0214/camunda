package io.camunda.zeebe.util.sched.akka.messages;

import java.util.Objects;

public final class Result<V> implements Message {
  private final V value;

  public Result(final V value) {
    this.value = value;
  }

  public V getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getValue());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Result)) {
      return false;
    }
    final Result<?> result = (Result<?>) o;
    return Objects.equals(getValue(), result.getValue());
  }

  @Override
  public String toString() {
    return "Result{" + "value=" + value + '}';
  }
}
