package io.camunda.zeebe.util.sched.akka;

import static org.assertj.core.api.Assertions.assertThat;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.Props;
import akka.actor.typed.Scheduler;
import akka.actor.typed.SpawnProtocol;
import akka.actor.typed.SpawnProtocol.Spawn;
import akka.actor.typed.internal.PoisonPill;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import io.camunda.zeebe.util.sched.akka.Protocol.Operation;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class ExampleTest {
  private ActorSystem<SpawnProtocol.Command> system;

  @BeforeEach
  void beforeEach() {
    system = ActorSystem.create(SpawnProtocol.create(), "example");
  }

  @Test
  void shouldSerializeStateChanges() throws InterruptedException {
    // given
    final Scheduler scheduler = system.scheduler();
    final Behavior<Protocol.Operation> behavior = Behaviors.setup(ControlActor::new);
    final CompletionStage<ActorRef<Protocol.Operation>> spawnResult =
        AskPattern.ask(
            system,
            replyTo -> new Spawn<>(behavior, "alice", Props.empty(), replyTo),
            Duration.ofSeconds(5),
            scheduler);
    final var actor = spawnResult.toCompletableFuture().join();
    final var control = new AkkaControl(actor, scheduler);
    final var alice = new Alice(control);

    // when
    alice.setCounter(10);

    // then
    assertThat(alice.getCounter()).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(10);
  }

  private static final class ControlActor extends AbstractBehavior<Protocol.Operation> {
    private final OperationBehavior operations;

    public ControlActor(final ActorContext<Protocol.Operation> context) {
      super(context);
      operations = new OperationBehavior(context);
    }

    @Override
    public Receive<Operation> createReceive() {
      return newReceiveBuilder()
          .onSignal(PoisonPill.class, this::onStop)
          .onAnyMessage(operation -> operations.receive(getContext(), operation))
          .build();
    }

    private Behavior<Protocol.Operation> onStop(final PoisonPill signal) {
      return Behaviors.stopped();
    }
  }

  private static final class Alice {
    private final AkkaControl control;

    private int counter = 0;

    public Alice(final AkkaControl control) {
      this.control = control;
    }

    public CompletionStage<Integer> getCounter() {
      return control.call(() -> counter);
    }

    public void setCounter(final int counter) {
      control.run(() -> this.counter = counter);
    }
  }
}
