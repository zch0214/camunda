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
package io.camunda.zeebe.client.impl.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.RestCredentialsProvider;
import io.camunda.zeebe.client.api.response.Topology;
import io.camunda.zeebe.client.impl.ZeebeClientBuilderImpl;
import io.camunda.zeebe.client.impl.command.TopologyRequestImpl;
import io.camunda.zeebe.client.impl.http.JsonAsyncResponseConsumer.JsonResponse;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.Future;
import org.apache.hc.client5.http.async.methods.ConfigurableHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin abstraction layer on top of Apache's HTTP client to wire up the expected Zeebe API
 * conventions, e.g. errors are always {@link io.camunda.zeebe.gateway.protocol.rest.ProblemDetail},
 * content type is always JSON, etc.
 */
public final class HttpClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);

  private final CloseableHttpAsyncClient client;
  private final ObjectMapper jsonMapper;
  private final URI address;
  private final RequestConfig defaultRequestConfig;
  private final int maxMessageSize;
  private final TimeValue shutdownTimeout;
  private final RestCredentialsProvider credentialsProvider;

  public HttpClient(
      final CloseableHttpAsyncClient client,
      final ObjectMapper jsonMapper,
      final URI address,
      final RequestConfig defaultRequestConfig,
      final int maxMessageSize,
      final TimeValue shutdownTimeout,
      final RestCredentialsProvider credentialsProvider) {
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.address = address;
    this.defaultRequestConfig = defaultRequestConfig;
    this.maxMessageSize = maxMessageSize;
    this.shutdownTimeout = shutdownTimeout;
    this.credentialsProvider = credentialsProvider;
  }

  public void start() {
    client.start();
  }

  @Override
  public void close() throws Exception {
    client.close(CloseMode.GRACEFUL);
    try {
      client.awaitShutdown(shutdownTimeout);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Expected to await HTTP client shutdown, but was interrupted; client may not be "
              + "completely shut down",
          e);
    }
  }

  public RequestConfig.Builder newRequestConfig() {
    return RequestConfig.copy(defaultRequestConfig);
  }

  public <HttpT, RespT> void get(
      final String path,
      final RequestConfig requestConfig,
      final Class<HttpT> responseType,
      final JsonResponseTransformer<HttpT, RespT> transformer,
      final HttpZeebeFuture<RespT> result,
      final Runnable retryAction) {
    final ConfigurableHttpRequest request = buildRequest(path, requestConfig);
    final JsonAsyncResponseConsumer<HttpT> responseConsumer =
        new JsonAsyncResponseConsumer<>(jsonMapper, responseType, maxMessageSize);
    final AuthCallback<HttpT, RespT> callback =
        new AuthCallback<>(
            credentialsProvider, new JsonCallback<>(result, transformer), retryAction);
    final Future<JsonResponse<HttpT>> httpFuture =
        client.execute(noBody(request), responseConsumer, callback);
    result.transportFuture(httpFuture);
  }

  // TODO: remove, as this is just meant for testing. I generated the certificates:
  // mkdir -p /tmp/certs && cd /tmp/certs && \
  // openssl req -config <(printf
  // "[req]\ndistinguished_name=dn\n[dn]\n[ext]\nbasicConstraints=CA:TRUE,pathlen:0") -new -newkey
  // rsa:2048 -nodes -subj "/C=DE/O=Test/OU=Test/ST=BE/CN=cluster.local" -x509 -extensions ext
  // -keyout ca.key -out ca.pem && \
  // openssl genrsa -f4 -out nodeA.key 4096 && \
  // openssl req -new -key nodeA.key -out nodeA.csr && \
  // openssl x509 -req -days 365 -in nodeA.csr -CA ca.pem -CAkey ca.key -set_serial 01 -extfile
  // <(printf "subjectAltName = IP.1:127.0.0.1") -out nodeA.pem && \
  // touch nodeAChain.pem && cat nodeA.pem > nodeAChain.pem && cat ca.pem >> nodeAChain.pem
  //
  // then start the broker with the following properties:
  // server.ssl.enabled=true
  // server.ssl.certificate=/tmp/certs/nodeA.pem
  // server.ssl.certificate-private-key=/tmp/certs/nodeA.key
  //
  // note that this implies that you now have way more config to do than before as an operator, but
  // we knew that when we opted against a port unified solution (i.e. Armeria)
  public static void main(final String[] args) throws InterruptedException {
    final ZeebeClientBuilderImpl builder = new ZeebeClientBuilderImpl();
    builder
        .gatewayAddress("localhost:8080")
        .defaultRequestTimeout(Duration.ofSeconds(2))
        .caCertificatePath("/tmp/certs/nodeAChain.pem")
        .overrideAuthority("127.0.0.1");
    final HttpClientFactory factory = new HttpClientFactory(builder);
    final HttpClient client = factory.createClient();
    client.start();

    final Topology topology = new TopologyRequestImpl(client).send().join();
    Thread.sleep(30_000);
    int a = 1;
  }

  private ConfigurableHttpRequest buildRequest(final String path, final RequestConfig config) {
    final URI target = buildRequestURI(path);

    final ConfigurableHttpRequest request = SimpleHttpRequest.create(Method.GET, target);
    request.setConfig(config);
    return request;
  }

  private URI buildRequestURI(final String path) {
    final URI target;
    try {
      target = new URIBuilder(address).appendPath(path).build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return target;
  }

  private AsyncRequestProducer noBody(final HttpRequest request) {
    return new BasicRequestProducer(request, null);
  }
}
