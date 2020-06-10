/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.operate.qa.util.migration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.containers.Network;
import junit.framework.AssertionFailedError;

public class TestContext {

  private File zeebeDataFolder;

  private Network network;
  private String externalElsHost;
  private Integer externalElsPort;
  private String internalElsHost;
  private Integer internalElsPort;

  private String externalZeebeContactPoint;
  private String internalZeebeContactPoint;

  private String externalOperateHost;
  private Integer externalOperatePort;

  private List<String> workflowsToAssert = new ArrayList<>();

  public File getZeebeDataFolder() {
    return zeebeDataFolder;
  }

  public void setZeebeDataFolder(File zeebeDataFolder) {
    this.zeebeDataFolder = zeebeDataFolder;
  }

  public Network getNetwork() {
    return network;
  }

  public void setNetwork(Network network) {
    this.network = network;
  }

  public String getExternalElsHost() {
    return externalElsHost;
  }

  public void setExternalElsHost(String externalElsHost) {
    this.externalElsHost = externalElsHost;
  }

  public Integer getExternalElsPort() {
    return externalElsPort;
  }

  public void setExternalElsPort(Integer externalElsPort) {
    this.externalElsPort = externalElsPort;
  }

  public String getInternalElsHost() {
    return internalElsHost;
  }

  public void setInternalElsHost(String internalElsHost) {
    this.internalElsHost = internalElsHost;
  }

  public Integer getInternalElsPort() {
    return internalElsPort;
  }

  public void setInternalElsPort(Integer internalElsPort) {
    this.internalElsPort = internalElsPort;
  }

  public String getExternalZeebeContactPoint() {
    return externalZeebeContactPoint;
  }

  public void setExternalZeebeContactPoint(String externalZeebeContactPoint) {
    this.externalZeebeContactPoint = externalZeebeContactPoint;
  }

  public String getInternalZeebeContactPoint() {
    return internalZeebeContactPoint;
  }

  public void setInternalZeebeContactPoint(String internalZeebeContactPoint) {
    this.internalZeebeContactPoint = internalZeebeContactPoint;
  }

  public String getExternalOperateHost() {
    return externalOperateHost;
  }

  public void setExternalOperateHost(String externalOperateHost) {
    this.externalOperateHost = externalOperateHost;
  }

  public Integer getExternalOperatePort() {
    return externalOperatePort;
  }

  public void setExternalOperatePort(Integer externalOperatePort) {
    this.externalOperatePort = externalOperatePort;
  }

  public List<String> getWorkflowsToAssert() {
    return workflowsToAssert;
  }

  public void setWorkflowsToAssert(List<String> workflowsToAssert) {
    this.workflowsToAssert = workflowsToAssert;
  }

  public void addWorkflow(String bpmnProcessId) {
    if (workflowsToAssert.contains(bpmnProcessId)) {
      throw new AssertionFailedError("Workflow was already created earlier: " + bpmnProcessId);
    }
    workflowsToAssert.add(bpmnProcessId);
  }
}
