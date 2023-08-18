/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.processors;

import io.camunda.zeebe.engine.perf.TestEngine;
import io.camunda.zeebe.engine.state.mutable.MutableProcessingState;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ProcessProcessorTest {

  private MutableProcessingState state;
  private TestEngine engine;

  @Test
  void test() throws IOException {
    engine = TestEngine.createSinglePartitionEngine();

    state = engine.getProcessingState();

    final Record<DeploymentRecordValue> deploy =
        engine
            .createDeploymentClient()
            .withXmlResource(
                Bpmn.createExecutableProcess("process")
                    .startEvent("none")
                    .serviceTask("task", t -> t.zeebeJobType("test"))
                    .moveToProcess("process")
                    .startEvent("message")
                    .message("a")
                    .done())
            .deploy();

    final long processInstanceKey =
        engine.createProcessInstanceClient().ofBpmnProcessId("process").create();

    state
        .getEventScopeInstanceState()
        .triggerStartEvent(
            deploy.getValue().getProcessesMetadata().get(0).getProcessDefinitionKey(),
            processInstanceKey,
            BufferUtil.wrapString("start"),
            BufferUtil.wrapArray(MsgPackConverter.convertToMsgPack("{}")),
            processInstanceKey);

    engine.createMessageClient().withName("a").withCorrelationKey("123").publish();

    RecordingExporter.jobRecords(JobIntent.CREATED).await();
  }
}
