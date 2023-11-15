/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.instance;

import static io.camunda.zeebe.protocol.record.Assertions.assertThat;

import io.camunda.zeebe.engine.state.mutable.MutableProcessingState;
import io.camunda.zeebe.engine.state.mutable.MutableUserTaskState;
import io.camunda.zeebe.engine.util.ProcessingStateRule;
import io.camunda.zeebe.msgpack.value.DocumentValue;
import io.camunda.zeebe.protocol.impl.record.value.usertask.UserTaskRecord;
import io.camunda.zeebe.test.util.BufferAssert;
import io.camunda.zeebe.test.util.MsgPackUtil;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class UserTaskStateTest {

  @Rule public final ProcessingStateRule stateRule = new ProcessingStateRule();

  private MutableUserTaskState userTaskState;

  @Before
  public void setUp() {
    final MutableProcessingState processingState = stateRule.getProcessingState();
    userTaskState = processingState.getUserTaskState();
  }

  @Test
  public void shouldCreateUserTask() {
    // given
    final UserTaskRecord expectedRecord = createUserTask();

    // when
    userTaskState.create(5_000, expectedRecord);

    // then
    final UserTaskRecord storedRecord = userTaskState.getUserTask(5_000);
    assertUserTask(expectedRecord, storedRecord);
  }

  @Test
  public void shouldNeverPersistUserTaskVariables() {
    // given
    final long key = 1L;
    final UserTaskRecord userTask = createUserTask();

    final List<BiConsumer<Long, UserTaskRecord>> stateUpdates =
        Arrays.asList(userTaskState::create, userTaskState::update);

    // when user task state is updated then the variables are not persisted
    for (final BiConsumer<Long, UserTaskRecord> stateUpdate : stateUpdates) {
      userTask.setVariables(MsgPackUtil.asMsgPack("foo", "bar"));
      stateUpdate.accept(key, userTask);
      final DirectBuffer variables = userTaskState.getUserTask(key).getVariablesBuffer();
      BufferAssert.assertThatBuffer(variables).isEqualTo(DocumentValue.EMPTY_DOCUMENT);
    }
  }

  private UserTaskRecord createUserTask() {
    return new UserTaskRecord()
        .setElementInstanceKey(1234)
        .setBpmnProcessId("process")
        .setElementId("process")
        .setProcessInstanceKey(4321)
        .setProcessDefinitionKey(8765)
        .setProcessDefinitionVersion(2)
        .setAssignee("myAssignee")
        .setCandidateGroups("myGroups")
        .setCandidateUsers("myUsers")
        .setDueDate("2023-11-11T11:11:00+01:00")
        .setFollowUpDate("2023-11-12T11:11:00+01:00")
        .setFormKey(5678);
  }

  private void assertUserTask(
      final UserTaskRecord expectedRecord, final UserTaskRecord storedRecord) {
    assertThat(storedRecord).hasElementInstanceKey(expectedRecord.getElementInstanceKey());
    assertThat(storedRecord).hasBpmnProcessId(expectedRecord.getBpmnProcessId());
    assertThat(storedRecord).hasElementId(expectedRecord.getElementId());
    assertThat(storedRecord).hasProcessInstanceKey(expectedRecord.getProcessInstanceKey());
    assertThat(storedRecord).hasProcessDefinitionKey(expectedRecord.getProcessDefinitionKey());
    assertThat(storedRecord)
        .hasProcessDefinitionVersion(expectedRecord.getProcessDefinitionVersion());

    assertThat(storedRecord).hasAssignee(expectedRecord.getAssignee());
    assertThat(storedRecord).hasCandidateGroups(expectedRecord.getCandidateGroups());
    assertThat(storedRecord).hasCandidateUsers(expectedRecord.getCandidateUsers());
    assertThat(storedRecord).hasDueDate(expectedRecord.getDueDate());
    assertThat(storedRecord).hasFollowUpDate(expectedRecord.getFollowUpDate());
    assertThat(storedRecord).hasFormKey(expectedRecord.getFormKey());

    assertThat(storedRecord).hasUserTaskKey(expectedRecord.getUserTaskKey());
    assertThat(storedRecord).hasTenantId(expectedRecord.getTenantId());
  }
}
