/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */
package io.camunda.operate.it.store;

import com.fasterxml.jackson.core.type.TypeReference;
import io.camunda.operate.entities.ProcessEntity;
import io.camunda.operate.entities.listview.ListViewJoinRelation;
import io.camunda.operate.entities.listview.ProcessInstanceForListViewEntity;
import io.camunda.operate.entities.listview.ProcessInstanceState;
import io.camunda.operate.schema.indices.ProcessIndex;
import io.camunda.operate.schema.templates.FlowNodeInstanceTemplate;
import io.camunda.operate.schema.templates.ListViewTemplate;
import io.camunda.operate.schema.templates.ProcessInstanceDependant;
import io.camunda.operate.schema.templates.SequenceFlowTemplate;
import io.camunda.operate.schema.templates.VariableTemplate;
import io.camunda.operate.store.ProcessStore;
import io.camunda.operate.util.j5templates.OperateSearchAbstractIT;
import io.camunda.operate.webapp.api.v1.entities.FlowNodeInstance;
import io.camunda.operate.webapp.api.v1.entities.ProcessDefinition;
import io.camunda.operate.webapp.api.v1.entities.SequenceFlow;
import io.camunda.operate.webapp.api.v1.entities.Variable;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.camunda.operate.schema.indices.IndexDescriptor.DEFAULT_TENANT_ID;
import static org.assertj.core.api.Assertions.assertThat;

public class ProcessStoreIT extends OperateSearchAbstractIT {

  @Autowired
  private ListViewTemplate listViewTemplate;

  @Autowired
  private List<ProcessInstanceDependant> processInstanceDependantTemplates;

  @Autowired
  private ProcessIndex processDefinitionIndex;

  @Autowired
  private ProcessStore processStore;

  private ProcessDefinition firstProcessDefinition;
  private ProcessDefinition secondProcessDefinition;
  private ProcessInstanceForListViewEntity firstProcessInstance;
  private ProcessInstanceForListViewEntity secondProcessInstance;
  private ProcessInstanceForListViewEntity thirdProcessInstance;

  @Override
  protected void runAdditionalBeforeAllSetup() throws Exception {
    firstProcessDefinition = new ProcessDefinition().setKey(2251799813685248L)
        .setBpmnProcessId("demoProcess").setTenantId(DEFAULT_TENANT_ID).setName("Demo process");
    secondProcessDefinition = new ProcessDefinition().setKey(2251799813685249L)
        .setBpmnProcessId("demoProcess-1").setTenantId(DEFAULT_TENANT_ID).setName("Demo process 1");
    List<ProcessDefinition> processDefinitionList = List.of(firstProcessDefinition, secondProcessDefinition);
    for (ProcessDefinition data : processDefinitionList) {
      Map<String, Object> entityMap = objectMapper.convertValue(data, new TypeReference<>() {
      });
      // XML source is not part of the model object and must be added manually in the entity map before writing
      String resourceXml = testResourceManager.readResourceFileContentsAsString("demoProcess_v_1.bpmn");
      entityMap.put("bpmnXml", resourceXml);
      entityMap.put("id", data.getKey());
      testSearchRepository.createOrUpdateDocument(processDefinitionIndex.getFullQualifiedName(), String.valueOf(data.getKey()), entityMap);
    }

    firstProcessInstance = new ProcessInstanceForListViewEntity()
        .setId("4503599627370497").setKey(4503599627370497L).setProcessDefinitionKey(firstProcessDefinition.getKey()).setProcessInstanceKey(4503599627370497L)
        .setProcessName("Demo process parent").setBpmnProcessId("demoProcess").setState(ProcessInstanceState.ACTIVE)
        .setTreePath("PI_4503599627370497").setTenantId(DEFAULT_TENANT_ID).setJoinRelation(new ListViewJoinRelation("processInstance"));
    testSearchRepository.createOrUpdateDocumentFromObject(listViewTemplate.getFullQualifiedName(), firstProcessInstance.getId(), firstProcessInstance);

    secondProcessInstance = new ProcessInstanceForListViewEntity()
        .setId("2251799813685251").setKey(2251799813685251L).setProcessDefinitionKey(secondProcessDefinition.getKey()).setProcessInstanceKey(2251799813685251L)
        .setParentProcessInstanceKey(4503599627370497L).setProcessName("Demo process child 1").setBpmnProcessId("demoProcess-1").setState(ProcessInstanceState.COMPLETED)
        .setTreePath("PI_2251799813685251").setTenantId(DEFAULT_TENANT_ID).setIncident(false).setJoinRelation(new ListViewJoinRelation("processInstance"));
    testSearchRepository.createOrUpdateDocumentFromObject(listViewTemplate.getFullQualifiedName(), secondProcessInstance.getId(), secondProcessInstance);

    thirdProcessInstance = new ProcessInstanceForListViewEntity()
        .setId("2251799813685252").setKey(2251799813685252L).setProcessDefinitionKey(secondProcessDefinition.getKey()).setProcessInstanceKey(2251799813685252L)
        .setParentProcessInstanceKey(4503599627370497L).setProcessName("Demo process child 2").setBpmnProcessId("demoProcess-1").setState(ProcessInstanceState.ACTIVE)
        .setTreePath("PI_2251799813685252").setTenantId(DEFAULT_TENANT_ID).setIncident(true).setJoinRelation(new ListViewJoinRelation("processInstance"));
    testSearchRepository.createOrUpdateDocumentFromObject(listViewTemplate.getFullQualifiedName(), thirdProcessInstance.getId(), thirdProcessInstance);

    searchContainerManager.refreshIndices("*");
  }

  @Test
  public void testGetDistinctCountFor() {
    final Optional<Long> optionalCount = processStore.getDistinctCountFor(ListViewTemplate.BPMN_PROCESS_ID);

    assertThat(optionalCount).isNotNull();
    assertThat(optionalCount.get()).isEqualTo(2L);
  }

  @Test
  public void testGetProcessByKey() {
    ProcessEntity result = processStore.getProcessByKey(secondProcessDefinition.getKey());

    assertThat(result).isNotNull();
    assertThat(result.getBpmnProcessId()).isEqualTo(secondProcessDefinition.getBpmnProcessId());
    assertThat(result.getKey()).isEqualTo(secondProcessDefinition.getKey());
  }

  @Test
  public void testGetDiagramByKey() {
    String xml = processStore.getDiagramByKey(secondProcessDefinition.getKey());
    assertThat(xml).isEqualTo(testResourceManager.readResourceFileContentsAsString("demoProcess_v_1.bpmn"));
  }

  @Test
  public void testGetProcessesGrouped() {
    Map<ProcessStore.ProcessKey, List<ProcessEntity>> results =
        processStore.getProcessesGrouped(DEFAULT_TENANT_ID, Set.of(firstProcessDefinition.getBpmnProcessId(),
            secondProcessDefinition.getBpmnProcessId()));

    assertThat(results.values().size()).isEqualTo(2);
    assertThat(results.get(new ProcessStore.ProcessKey(firstProcessDefinition.getBpmnProcessId(), DEFAULT_TENANT_ID)).size()).isEqualTo(1);
    assertThat(results.get(new ProcessStore.ProcessKey(secondProcessDefinition.getBpmnProcessId(), DEFAULT_TENANT_ID)).size()).isEqualTo(1);
  }

  @Test
  public void testGetProcessesIdsToProcessesWithFields() {
    Map<Long, ProcessEntity> results = processStore.getProcessesIdsToProcessesWithFields(Set.of(firstProcessDefinition.getBpmnProcessId(), secondProcessDefinition.getBpmnProcessId()), 10, "name", "bpmnProcessId", "key");
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(firstProcessDefinition.getKey()).getBpmnProcessId()).isEqualTo(firstProcessDefinition.getBpmnProcessId());
    assertThat(results.get(secondProcessDefinition.getKey()).getBpmnProcessId()).isEqualTo(secondProcessDefinition.getBpmnProcessId());
  }

  @Test
  public void testGetProcessInstanceListViewByKey() {
    ProcessInstanceForListViewEntity result = processStore.getProcessInstanceListViewByKey(
        thirdProcessInstance.getProcessInstanceKey());

    assertThat(result).isNotNull();
    assertThat(result.getBpmnProcessId()).isEqualTo(thirdProcessInstance.getBpmnProcessId());
    assertThat(result.getKey()).isEqualTo(thirdProcessInstance.getProcessInstanceKey());
  }

  @Test
  public void testGetCoreStatistics() {
    Map<String, Long> results = processStore.getCoreStatistics(Set.of("demoProcess-1"));
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get("running")).isEqualTo(1);
    assertThat(results.get("incidents")).isEqualTo(1);
  }

  @Test
  public void testGetProcessInstanceTreePathById() {
    String result = processStore.getProcessInstanceTreePathById(thirdProcessInstance.getId());
    assertThat(result).isEqualTo(thirdProcessInstance.getTreePath());
  }

  @Test
  public void testDeleteDocument() throws IOException {
    Long processKey = 2951799813685251L;
    testSearchRepository.createOrUpdateDocumentFromObject(listViewTemplate.getFullQualifiedName(),
        new ProcessInstanceForListViewEntity().setKey(processKey).setId(String.valueOf(processKey))
            .setBpmnProcessId("fakeProcess").setTenantId(DEFAULT_TENANT_ID));

    searchContainerManager.refreshIndices("*operate-list*");

    long deleted = processStore.deleteDocument(listViewTemplate.getFullQualifiedName(), ListViewTemplate.KEY, String.valueOf(processKey));

    searchContainerManager.refreshIndices("*operate-list*");

    var response = testSearchRepository.searchTerm(listViewTemplate.getFullQualifiedName(), ListViewTemplate.PROCESS_INSTANCE_KEY, processKey, Object.class, 1);
    assertThat(response.size()).isZero();
    assertThat(deleted).isEqualTo(1L);
  }

  @Test
  public void testGetProcessInstancesByProcessAndStates() {
    List<ProcessInstanceForListViewEntity> results = processStore.getProcessInstancesByProcessAndStates(
        secondProcessDefinition.getKey(), Set.of(ProcessInstanceState.COMPLETED), 10, null);

    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getProcessDefinitionKey()).isEqualTo(secondProcessDefinition.getKey());
    assertThat(results.get(0).getState()).isEqualTo(ProcessInstanceState.COMPLETED);
  }

  @Test
  public void testGetProcessInstancesByProcessAndStatesWithMultipleStates() {
    List<ProcessInstanceForListViewEntity> results = processStore.getProcessInstancesByProcessAndStates(
        secondProcessDefinition.getKey(), Set.of(ProcessInstanceState.COMPLETED, ProcessInstanceState.ACTIVE), 10, null);

    assertThat(results.size()).isEqualTo(2);
    assertThat(results.stream().filter(data ->
        data.getProcessDefinitionKey().equals(secondProcessDefinition.getKey()) &&
        data.getState().equals(ProcessInstanceState.COMPLETED)).findAny().orElse(null))
        .isNotNull();
    assertThat(results.stream().filter(data ->
        data.getProcessDefinitionKey().equals(secondProcessDefinition.getKey()) &&
            data.getState().equals(ProcessInstanceState.ACTIVE)).findAny().orElse(null))
        .isNotNull();
  }

  @Test
  public void testGetProcessInstancesByParentKeys() {
    List<ProcessInstanceForListViewEntity> results = processStore.getProcessInstancesByParentKeys(
        Set.of(secondProcessInstance.getParentProcessInstanceKey()), 10, null);

    assertThat(results.size()).isEqualTo(2);
    assertThat(results.stream().filter(data ->
        data.getProcessInstanceKey().equals(secondProcessInstance.getProcessInstanceKey()) &&
            data.getParentProcessInstanceKey().equals(secondProcessInstance.getParentProcessInstanceKey())).findAny().orElse(null))
        .isNotNull();
    assertThat(results.stream().filter(data ->
        data.getProcessInstanceKey().equals(thirdProcessInstance.getProcessInstanceKey()) &&
            data.getParentProcessInstanceKey().equals(thirdProcessInstance.getParentProcessInstanceKey())).findAny().orElse(null))
        .isNotNull();
  }

  @Test
  public void testDeleteProcessDefinitionsByKeys() throws IOException {
    testSearchRepository.createOrUpdateDocumentFromObject(processDefinitionIndex.getFullQualifiedName(), new ProcessDefinition().setKey(2251799813685298L)
        .setBpmnProcessId("testProcess1").setTenantId(DEFAULT_TENANT_ID).setName("Test process 1"));
    testSearchRepository.createOrUpdateDocumentFromObject(processDefinitionIndex.getFullQualifiedName(), new ProcessDefinition().setKey(2251799813685299L)
        .setBpmnProcessId("testProcess2").setTenantId(DEFAULT_TENANT_ID).setName("Test process 2"));
    searchContainerManager.refreshIndices("*operate-process*");

    long deleted = processStore.deleteProcessDefinitionsByKeys(2251799813685298L, 2251799813685299L);
    assertThat(deleted).isEqualTo(2);

    searchContainerManager.refreshIndices("*operate-process*");
  }

  @Test
  public void testDeleteProcessInstancesAndDependants() throws IOException {
    Long processKey = 2951799813685251L;
    testSearchRepository.createOrUpdateDocumentFromObject(listViewTemplate.getFullQualifiedName(),
        new ProcessInstanceForListViewEntity().setKey(processKey).setId(String.valueOf(processKey))
            .setBpmnProcessId("fakeProcess").setTenantId(DEFAULT_TENANT_ID));

    testSearchRepository.createOrUpdateDocumentFromObject(getFullIndexNameForDependant(FlowNodeInstanceTemplate.INDEX_NAME),
        new FlowNodeInstance().setProcessInstanceKey(processKey));
    testSearchRepository.createOrUpdateDocumentFromObject(getFullIndexNameForDependant(SequenceFlowTemplate.INDEX_NAME),
        new SequenceFlow().setProcessInstanceKey(processKey));
    testSearchRepository.createOrUpdateDocumentFromObject(getFullIndexNameForDependant(VariableTemplate.INDEX_NAME),
        new Variable().setProcessInstanceKey(processKey));

    searchContainerManager.refreshIndices("*");

    long deleted = processStore.deleteProcessInstancesAndDependants(Set.of(processKey));

    searchContainerManager.refreshIndices("*");
    var response = testSearchRepository.searchTerm(listViewTemplate.getFullQualifiedName(), ListViewTemplate.PROCESS_INSTANCE_KEY, processKey, Object.class, 1);
    assertThat(response.size()).isZero();
    assertThat(deleted).isEqualTo(4);
  }

  private String getFullIndexNameForDependant(String indexName) {
    ProcessInstanceDependant dependant = processInstanceDependantTemplates.stream().filter(template ->
        template.getFullQualifiedName().contains(indexName)).findAny().orElse(null);

    return dependant.getFullQualifiedName();
  }
}