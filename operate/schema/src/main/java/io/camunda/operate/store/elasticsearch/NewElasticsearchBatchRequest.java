/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.operate.store.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import io.camunda.operate.entities.OperateEntity;
import io.camunda.operate.exceptions.PersistenceException;
import io.camunda.operate.store.BatchRequest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch request that uses new style ElasticsearchClient instead of deprecated RestHighLevelClient.
 */
public class NewElasticsearchBatchRequest implements BatchRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(NewElasticsearchBatchRequest.class);

  private final BulkRequest.Builder bulkRequestBuilder = new BulkRequest.Builder();

  private final ElasticsearchClient esClient;

  public NewElasticsearchBatchRequest(final ElasticsearchClient esClient) {
    this.esClient = esClient;
  }

  @Override
  public BatchRequest add(final String index, final OperateEntity entity)
      throws PersistenceException {
    return addWithId(index, entity.getId(), entity);
  }

  @Override
  public BatchRequest addWithId(final String index, final String id, final OperateEntity entity)
      throws PersistenceException {
    LOGGER.debug("Add index request for index {} id {} and entity {} ", index, id, entity);
    bulkRequestBuilder.operations(op -> op.index(idx -> idx.index(index).id(id).document(entity)));
    return this;
  }

  @Override
  public BatchRequest addWithRouting(
      final String index, final OperateEntity entity, final String routing)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BatchRequest upsert(
      final String index,
      final String id,
      final OperateEntity entity,
      final Map<String, Object> updateFields)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BatchRequest upsertWithRouting(
      final String index,
      final String id,
      final OperateEntity entity,
      final Map<String, Object> updateFields,
      final String routing)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BatchRequest upsertWithScript(
      final String index,
      final String id,
      final OperateEntity entity,
      final String script,
      final Map<String, Object> parameters)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BatchRequest upsertWithScriptAndRouting(
      final String index,
      final String id,
      final OperateEntity entity,
      final String script,
      final Map<String, Object> parameters,
      final String routing)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BatchRequest update(
      final String index, final String id, final Map<String, Object> updateFields)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BatchRequest update(final String index, final String id, final OperateEntity entity)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BatchRequest updateWithScript(
      final String index,
      final String id,
      final String script,
      final Map<String, Object> parameters)
      throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void execute() throws PersistenceException {
    final BulkRequest bulkRequest = bulkRequestBuilder.build();
    try {
      LOGGER.debug("************* FLUSH BULK START *************");
      final BulkResponse bulkResponse = esClient.bulk(bulkRequest);
      final List<BulkResponseItem> items = bulkResponse.items();
      for (final BulkResponseItem item : items) {
        if (item.error() != null) {
          LOGGER.error(
              String.format("Bulk request execution failed. %s. Cause: %s.", item),
              item.error().reason());
          throw new PersistenceException("Operation failed: " + item.error().reason());
        }
      }
      LOGGER.debug("************* FLUSH BULK FINISH *************");
    } catch (final IOException ex) {
      throw new PersistenceException(
          "Error when processing bulk request against Elasticsearch: " + ex.getMessage(), ex);
    }
  }

  @Override
  public void executeWithRefresh() throws PersistenceException {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
