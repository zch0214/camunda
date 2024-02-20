/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.client.impl.command;

import io.camunda.zeebe.client.api.ZeebeFuture;
import io.camunda.zeebe.client.api.command.FinalCommandStep;
import io.camunda.zeebe.client.api.command.TopologyRequestStep1;
import io.camunda.zeebe.client.api.response.Topology;
import io.camunda.zeebe.client.impl.http.HttpClient;
import io.camunda.zeebe.client.impl.http.HttpZeebeFuture;
import io.camunda.zeebe.client.impl.response.TopologyImpl;
import io.camunda.zeebe.gateway.protocol.rest.TopologyResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.config.RequestConfig;

public final class TopologyRequestImpl implements TopologyRequestStep1 {

  private final HttpClient client;
  private final RequestConfig.Builder requestConfig;

  public TopologyRequestImpl(final HttpClient client) {
    this.client = client;
    this.requestConfig = client.newRequestConfig();
  }

  @Override
  public FinalCommandStep<Topology> requestTimeout(final Duration requestTimeout) {
    requestConfig.setResponseTimeout(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
    return this;
  }

  @Override
  public ZeebeFuture<Topology> send() {
    final HttpZeebeFuture<Topology> result = new HttpZeebeFuture<>();
    sendRequest(result);
    return result;
  }

  private void sendRequest(final HttpZeebeFuture<Topology> result) {
    client.get(
        "/topology",
        requestConfig.build(),
        TopologyResponse.class,
        TopologyImpl::new,
        result,
        () -> sendRequest(result));
  }
}
