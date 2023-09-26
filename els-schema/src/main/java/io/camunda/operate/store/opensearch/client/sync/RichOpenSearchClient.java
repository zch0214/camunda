/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */
package io.camunda.operate.store.opensearch.client.sync;

import io.camunda.operate.conditions.OpensearchCondition;
import io.camunda.operate.store.opensearch.client.async.OpenSearchAsyncDocumentOperations;
import io.camunda.operate.store.opensearch.client.async.OpenSearchAsyncIndexOperations;
import io.camunda.operate.store.opensearch.client.async.OpenSearchAsyncTaskOperations;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;


@Conditional(OpensearchCondition.class)
@Component
public class RichOpenSearchClient {
  public record AggregationValue(String key, long count){}

  private static final Logger LOGGER = LoggerFactory.getLogger(RichOpenSearchClient.class);

  BeanFactory beanFactory;
  OpenSearchClient openSearchClient;
  final Async async;
  final OpenSearchBatchOperations openSearchBatchOperations;
  final OpenSearchClusterOperations openSearchClusterOperations;
  final OpenSearchDocumentOperations openSearchDocumentOperations;
  final OpenSearchIndexOperations openSearchIndexOperations;
  final OpenSearchPipelineOperations openSearchPipelineOperations;
  final OpenSearchTaskOperations openSearchTaskOperations;
  final OpenSearchTemplateOperations openSearchTemplateOperations;

  public class Async {
    final OpenSearchAsyncDocumentOperations openSearchAsyncDocumentOperations;
    final OpenSearchAsyncIndexOperations openSearchAsyncIndexOperations;
    final OpenSearchAsyncTaskOperations openSearchAsyncTaskOperations;

    public Async(OpenSearchAsyncClient openSearchAsyncClient) {
      this.openSearchAsyncDocumentOperations = new OpenSearchAsyncDocumentOperations(LOGGER, openSearchAsyncClient);
      this.openSearchAsyncIndexOperations = new OpenSearchAsyncIndexOperations(LOGGER, openSearchAsyncClient);
      this.openSearchAsyncTaskOperations = new OpenSearchAsyncTaskOperations(LOGGER, openSearchAsyncClient);
    }

    public OpenSearchAsyncDocumentOperations doc() {
      return openSearchAsyncDocumentOperations;
    }

    public OpenSearchAsyncIndexOperations index() {
      return openSearchAsyncIndexOperations;
    }

    public OpenSearchAsyncTaskOperations task() {
      return openSearchAsyncTaskOperations;
    }
  }

  public RichOpenSearchClient(BeanFactory beanFactory, OpenSearchClient openSearchClient, OpenSearchAsyncClient openSearchAsyncClient) {
    this.beanFactory = beanFactory;
    this.openSearchClient = openSearchClient;
    async = new Async(openSearchAsyncClient);
    openSearchBatchOperations = new OpenSearchBatchOperations(LOGGER, openSearchClient, beanFactory);
    openSearchClusterOperations = new OpenSearchClusterOperations(LOGGER, openSearchClient);
    openSearchDocumentOperations = new OpenSearchDocumentOperations(LOGGER, openSearchClient);
    openSearchIndexOperations = new OpenSearchIndexOperations(LOGGER, openSearchClient);
    openSearchPipelineOperations = new OpenSearchPipelineOperations(LOGGER, openSearchClient);
    openSearchTaskOperations = new OpenSearchTaskOperations(LOGGER, openSearchClient);
    openSearchTemplateOperations = new OpenSearchTemplateOperations(LOGGER, openSearchClient);
  }

  public Async async() {
    return async;
  }

  public OpenSearchBatchOperations batch() {
    return openSearchBatchOperations;
  }

  public OpenSearchClusterOperations cluster() {
    return openSearchClusterOperations;
  }

  public OpenSearchDocumentOperations doc() {
    return openSearchDocumentOperations;
  }

  public OpenSearchIndexOperations index() {
    return openSearchIndexOperations;
  }

  public OpenSearchPipelineOperations pipeline() {
    return openSearchPipelineOperations;
  }

  public OpenSearchTaskOperations task() {
    return openSearchTaskOperations;
  }

  public OpenSearchTemplateOperations template() {
    return openSearchTemplateOperations;
  }
}