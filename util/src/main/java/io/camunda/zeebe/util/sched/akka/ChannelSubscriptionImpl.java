package io.camunda.zeebe.util.sched.akka;

import io.camunda.zeebe.util.sched.ActorCondition;
import io.camunda.zeebe.util.sched.channel.ChannelSubscription;
import io.camunda.zeebe.util.sched.channel.ConsumableChannel;

/** A cancellable {@link ChannelSubscription} implementation which wraps a cancellable task. */
public final class ChannelSubscriptionImpl implements ActorCondition, ChannelSubscription {
  private final ConsumableChannel channel;
  private final CancellableTask task;

  public ChannelSubscriptionImpl(final ConsumableChannel channel, final CancellableTask task) {
    this.channel = channel;
    this.task = task;
  }

  @Override
  public void signal() {
    task.run();
  }

  @Override
  public void cancel() {
    channel.removeConsumer(this);
    task.cancel();
  }
}
