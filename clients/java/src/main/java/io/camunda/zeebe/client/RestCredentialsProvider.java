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
package io.camunda.zeebe.client;

import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProvider;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProviderBuilder;
import java.io.IOException;
import java.util.Map;

public interface RestCredentialsProvider {
  RestCredentialsProvider NOOP = new NoopRestCredentialsProvider();

  /**
   * Adds credentials to the headers.
   *
   * @param headers map of existing headers to be modified
   */
  void applyCredentials(Map<String, String> headers) throws IOException;

  /**
   * Returns true if the request should be retried; otherwise returns false.
   *
   * @param status the status code returned by the failed request
   */
  boolean shouldRetryRequest(final int status);

  /**
   * @return a builder to configure and create a new {@link OAuthCredentialsProvider}.
   */
  static OAuthCredentialsProviderBuilder newCredentialsProviderBuilder() {
    return new OAuthCredentialsProviderBuilder();
  }

  class NoopRestCredentialsProvider implements RestCredentialsProvider {

    @Override
    public void applyCredentials(final Map<String, String> headers) throws IOException {}

    @Override
    public boolean shouldRetryRequest(final int status) {
      return false;
    }
  }
}
