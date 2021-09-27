package io.camunda.zeebe.util.sched.akka.messages;

public final class Close implements Message {
  public static final class Closing implements Message {}
  public static final class Closed implements Message {}
}
