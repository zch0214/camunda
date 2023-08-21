/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.processors;

import io.camunda.zeebe.engine.perf.TestEngine;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RecordingExporterExtension.class)
public class ProcessProcessorTest {

  @Test
  @DisplayName("should find specific event trigger in ProcessProcessor")
  void shouldFindSpecificEventTrigger() throws IOException {
    final var engine = TestEngine.createSinglePartitionEngine();
    final var state = engine.getProcessingState();

    // given a deployed process
    final var deployedProcess =
        engine
            .createDeploymentClient()
            .withXmlResource(
                Bpmn.createExecutableProcess("process").startEvent("message").message("a").done())
            .deploy()
            .getValue()
            .getProcessesMetadata()
            .get(0);

    // and an addition irrelevant event trigger on that process
    final long someOtherEventKey = 123L;
    state
        .getEventScopeInstanceState()
        .triggerStartEvent(
            deployedProcess.getProcessDefinitionKey(),
            someOtherEventKey,
            BufferUtil.wrapString("start"),
            BufferUtil.wrapArray(MsgPackConverter.convertToMsgPack("{}")),
            someOtherEventKey);

    // when publishing a message
    engine.createMessageClient().withName("a").withCorrelationKey("123").publish();

    // then the process instance is executed
    Assertions.assertThat(
            RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .withElementType(BpmnElementType.PROCESS)
                .limitToProcessInstanceCompleted())
        .describedAs("Expect that a process instance is executed")
        .hasSize(1);
  }
}
