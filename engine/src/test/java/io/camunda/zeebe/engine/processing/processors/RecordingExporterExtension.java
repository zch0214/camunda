/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.processors;

import io.camunda.zeebe.test.util.record.RecordLogger;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

/**
 * Use this extension to reset the RecordingExporter in between tests and automatically log all
 * records after one of them failed.
 *
 * <p>This JUnit 5 extension replaces the JUnit 4 test watcher {@link
 * io.camunda.zeebe.test.util.record.RecordingExporterTestWatcher}.
 *
 * <p>Usage:
 *
 * <pre>
 *   &#64;ExtendWith(RecordingExporterExtension.class)
 *   public class MyTest {
 *     &#64;Test
 *     public void test() {
 *       final var engine = TestEngine.createSinglePartitionEngine();
 *       // ...
 *     }
 *   }
 *   </pre>
 */
public final class RecordingExporterExtension implements BeforeEachCallback, TestWatcher {

  @Override
  public void testFailed(final ExtensionContext context, final Throwable cause) {
    RecordLogger.logRecords();
  }

  @Override
  public void beforeEach(final ExtensionContext extensionContext) throws Exception {
    RecordingExporter.reset();
  }
}
