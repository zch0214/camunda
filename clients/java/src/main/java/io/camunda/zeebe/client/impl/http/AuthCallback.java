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

import io.camunda.zeebe.client.RestCredentialsProvider;
import io.camunda.zeebe.client.impl.http.JsonAsyncResponseConsumer.JsonResponse;
import org.apache.hc.core5.concurrent.FutureCallback;

final class AuthCallback<HttpT, RespT> implements FutureCallback<JsonResponse<HttpT>> {
  private final RestCredentialsProvider credentialsProvider;
  private final JsonCallback<HttpT, RespT> delegate;
  private final Runnable retryAction;

  AuthCallback(
      final RestCredentialsProvider credentialsProvider,
      final JsonCallback<HttpT, RespT> delegate,
      final Runnable retryAction) {
    this.credentialsProvider = credentialsProvider;
    this.delegate = delegate;
    this.retryAction = retryAction;
  }

  @Override
  public void completed(final JsonResponse<HttpT> result) {
    if (credentialsProvider.shouldRetryRequest(result.getCode())) {
      // retry for authentication
      // authenticate and retry the request
      retryAction.run();
      return;
    }

    delegate.completed(result);
  }

  @Override
  public void failed(final Exception ex) {
    delegate.failed(ex);
  }

  @Override
  public void cancelled() {
    delegate.cancelled();
  }
}
