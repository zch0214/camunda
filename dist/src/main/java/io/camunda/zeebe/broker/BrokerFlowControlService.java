/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker;

import com.netflix.concurrency.limits.Limit;
import io.camunda.zeebe.logstreams.impl.flowcontrol.LimitType;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component(value = "flowControlService")
public class BrokerFlowControlService {

  private final SpringBrokerBridge bridge;

  @Autowired
  public BrokerFlowControlService(final SpringBrokerBridge bridge) {
    this.bridge = bridge;
  }

  public Map<Integer, Map<LimitType, Limit>> get() {
    return Map.of();
  }

  public void set() {
    //    bridge.getAdminService().map(adminService -> adminService.softPauseExporting()).join();
  }
}
