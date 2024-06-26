/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Loggers {
  public static final Logger STREAM_PROCESSING =
      LoggerFactory.getLogger("io.camunda.zeebe.broker.logstreams");

  public static final Logger PROCESS_PROCESSOR_LOGGER =
      LoggerFactory.getLogger("io.camunda.zeebe.broker.process");

  public static final Logger ENGINE_PROCESSING_LOGGER =
      LoggerFactory.getLogger("io.camunda.zeebe.engine.processing");

  public static Logger getExporterLogger(final String exporterId) {
    final String loggerName = String.format("io.camunda.zeebe.broker.exporter.%s", exporterId);
    return LoggerFactory.getLogger(loggerName);
  }
}
