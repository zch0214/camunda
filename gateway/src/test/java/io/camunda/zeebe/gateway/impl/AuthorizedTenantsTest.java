/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl;

import static io.camunda.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.camunda.identity.sdk.tenants.dto.Tenant;
import io.camunda.zeebe.auth.impl.Authorization;
import io.camunda.zeebe.gateway.api.util.GatewayTest;
import io.camunda.zeebe.gateway.api.util.StubbedBrokerClient;
import io.camunda.zeebe.gateway.api.util.StubbedBrokerClient.RequestStub;
import io.camunda.zeebe.gateway.impl.broker.request.BrokerDeployResourceRequest;
import io.camunda.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass.DeployResourceRequest;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import java.util.List;
import org.agrona.DirectBuffer;
import org.junit.Test;

public class AuthorizedTenantsTest extends GatewayTest {

  public AuthorizedTenantsTest() {
    super(cfg -> cfg.getMultiTenancy().setEnabled(true));
  }

  @Test
  public void test() {
    // given
    when(gateway.identity().tenants().forToken(anyString()))
        .thenReturn(List.of(new Tenant("tenant-a", "A"), new Tenant("tenant-b", "B")));
    final FakeRequestStub stub = new FakeRequestStub();
    stub.registerWith(brokerClient);

    // when
    final var request = DeployResourceRequest.newBuilder().setTenantId("tenant-b").build();
    client.deployResource(request);

    // then
    final BrokerDeployResourceRequest brokerRequest = brokerClient.getSingleBrokerRequest();
    assertThat(brokerRequest.getAuthorization().toDecodedMap())
        .hasEntrySatisfying(
            Authorization.AUTHORIZED_TENANTS,
            v -> assertThat(v).asList().contains("tenant-a", "tenant-b"));
  }

  public static final class FakeRequestStub
      implements RequestStub<BrokerDeployResourceRequest, BrokerResponse<DeploymentRecord>> {

    private static final long KEY = 123;
    private static final long PROCESS_KEY = 456;
    private static final int PROCESS_VERSION = 789;
    private static final DirectBuffer CHECKSUM = wrapString("checksum");

    @Override
    public void registerWith(final StubbedBrokerClient gateway) {
      gateway.registerHandler(BrokerDeployResourceRequest.class, this);
    }

    protected long getKey() {
      return KEY;
    }

    protected long getProcessDefinitionKey() {
      return PROCESS_KEY;
    }

    public int getProcessVersion() {
      return PROCESS_VERSION;
    }

    @Override
    public BrokerResponse<DeploymentRecord> handle(final BrokerDeployResourceRequest request)
        throws Exception {
      final DeploymentRecord deploymentRecord = request.getRequestWriter();
      deploymentRecord
          .resources()
          .iterator()
          .forEachRemaining(
              r -> {
                deploymentRecord
                    .processesMetadata()
                    .add()
                    .setBpmnProcessId(r.getResourceNameBuffer())
                    .setResourceName(r.getResourceNameBuffer())
                    .setVersion(PROCESS_VERSION)
                    .setKey(PROCESS_KEY)
                    .setChecksum(CHECKSUM)
                    .setTenantId(deploymentRecord.getTenantId());
              });
      return new BrokerResponse<>(deploymentRecord, 0, KEY);
    }
  }
}
