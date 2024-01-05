/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.rest;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.util.AttributeKey;
import java.util.List;

public final class TenantProviderDecorator implements DecoratingHttpServiceFunction {
  static final AttributeKey<List<String>> TENANT_CTX_KEY =
      AttributeKey.newInstance("io.camunda.zeebe:authorized_tenants");
  private static final List<String> TENANTS = List.of("foo", "bar");

  @Override
  public HttpResponse serve(
      final HttpService delegate, final ServiceRequestContext ctx, final HttpRequest req)
      throws Exception {
    ctx.setAttr(TENANT_CTX_KEY, TENANTS);
    return delegate.serve(ctx, req);
  }
}
