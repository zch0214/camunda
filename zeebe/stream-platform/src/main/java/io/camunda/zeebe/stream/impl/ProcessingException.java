/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.stream.impl;

import io.camunda.zeebe.logstreams.log.LoggedEvent;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;

public final class ProcessingException extends RuntimeException {

  public ProcessingException(
      final String message,
      final LoggedEvent event,
      final RecordMetadata metadata,
      final Throwable cause) {
    super(formatMessage(message, event, metadata), cause);
  }

  private static String formatMessage(
      final String message, final LoggedEvent event, final RecordMetadata metadata) {
    return String.format("%s [%s %s]", message, formatEvent(event), formatMetadata(metadata));
  }

  private static String formatEvent(final LoggedEvent event) {
    if (event == null) {
      return "LoggedEvent [null]";
    }
    return event.toString();
  }

  private static String formatMetadata(final RecordMetadata metadata) {
    if (metadata == null) {
      return "RecordMetadata{null}";
    }
    return metadata.toString();
  }
}
