/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.message;

import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.engine.util.RecordToWrite;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.camunda.zeebe.protocol.record.intent.MessageIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.test.util.MsgPackUtil;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

public final class MessageStartEventTest2 {

  @ClassRule
  public static final EngineRule engine = EngineRule.multiplePartition(3).maxCommandsInBatch(100);

  @Rule public final TestWatcher watcher = new RecordingExporterTestWatcher();

  @BeforeClass
  public static void setup() {
    engine
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("wf")
                .startEvent("start")
                .message("a")
                .serviceTask("task", t -> t.zeebeJobType("test"))
                .done())
        .deploy();
  }

  // What do we need?
  // - we want to verify that the start event is triggered even when several event triggers exist
  // - we can first trigger the start event of the same process definition
  // - then we can trigger the start event of a different process definition
  // -
  @Test
  public void shouldCreateMany() {

    // given
    RecordingExporter.reset();

    // when - publish two messages concurrently
    final int i = (int) Math.ceil(Math.random() * 10000);
    //    engine.writeRecords(createMessages(i));
    for (int j = 0; j < i; j++) {
      engine
          .message()
          .expectNothing()
          .onPartition((int) Math.ceil(Math.random() * 3))
          .withName("a")
          .withTimeToLive(0)
          .withCorrelationKey("key-" + j)
          .publish();
    }

    // then
    Assertions.assertThat(
            RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATED)
                .withElementType(BpmnElementType.START_EVENT)
                .limit(i)
                .asList())
        .hasSize(i);
  }

  private static RecordToWrite[] createMessages(final int i) {
    return IntStream.range(0, i)
        .mapToObj(
            j -> RecordToWrite.command().message(MessageIntent.PUBLISH, newMessage("key-" + j)))
        .toArray(RecordToWrite[]::new);
  }

  @NotNull
  private static MessageRecord newMessage(final String correlationKey) {
    return new MessageRecord()
        .setName("a")
        .setTimeToLive(0L)
        .setCorrelationKey(correlationKey)
        .setVariables(MsgPackUtil.asMsgPack("correlation_key", correlationKey));
  }
}
