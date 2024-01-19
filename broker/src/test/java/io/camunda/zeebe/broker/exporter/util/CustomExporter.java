/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.exporter.util;

import io.camunda.zeebe.broker.Loggers;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Context.RecordFilter;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import java.time.Duration;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;

public class CustomExporter implements Exporter {

  private static final Logger LOGGER = Loggers.SYSTEM_LOGGER;

  private Deque toCommitToZeebe;
  private Context context;
  private Controller controller;

  private final List<Record<?>> exportedRecords = new CopyOnWriteArrayList<>();

  private int position = 1;

  public List<Record<?>> getExportedRecords() {
    return exportedRecords;
  }

  @Override
  public void configure(final Context context) {
    toCommitToZeebe = new ConcurrentLinkedDeque<>();
    this.context = context;

    context.setFilter(new RecordFilter() {
      @Override
      public boolean acceptType(final RecordType recordType) {
        return true;
      }

      @Override
      public boolean acceptValue(final ValueType valueType) {
        return ValueType.JOB.equals(valueType);
      }
    });
  }

  @Override
  public void open(final Controller controller) {
    toCommitToZeebe = new ConcurrentLinkedDeque<>();
    this.controller = controller;
    // initialize KafkaExporter components.
    updateRecordPosition();
  }

  @Override
  public void export(final Record<?> r) {
    if (r.getIntent().equals(JobIntent.CREATED)) {
      LOGGER.info("EXPORT RECORD");
      exportedRecords.add(r);
      toCommitToZeebe.add(r);
      // send to Kafka
      // add the record toCommitToZeebe        }    }

    }
  }

  private void updateRecordPosition() {
    LOGGER.info("UPDATE POSITION");
    try {
      // pop all records send to Kafka and and
      // if there is something to commit, commit the one with highest position
      // (During the first call from open() there is nothing to commit, since there is nothing send
      // yet.)
      if (!toCommitToZeebe.isEmpty()) {
        final var toCommit = toCommitToZeebe.pop();
        if (toCommit != null) {
          controller.updateLastExportedRecordPosition(position++);
        }
      }
    } finally {
      // reschedule it
      controller.scheduleCancellableTask(Duration.ofMillis(10), this::updateRecordPosition);
    }
  }
}
