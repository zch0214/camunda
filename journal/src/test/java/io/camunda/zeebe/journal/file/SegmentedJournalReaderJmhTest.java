/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.file;

import io.camunda.zeebe.journal.JournalReader;
import io.camunda.zeebe.journal.util.MockJournalMetastore;
import io.camunda.zeebe.util.buffer.BufferWriter;
import io.camunda.zeebe.util.buffer.DirectBufferWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.springframework.util.unit.DataSize;

public class SegmentedJournalReaderJmhTest {

  @Benchmark
  @Fork(value = 1, warmups = 1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void seekToLast(final JournalWithIndex journal) {
    testSeek(journal.reader, journal.journal);
  }

  private static void testSeek(final JournalReader reader, final SegmentedJournal journal) {
    reader.seekToFirst();
    // seek to last entry for first segment
    reader.seek(journal.getLastSegment().index() - 1);
  }

  public static void main(final String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }

  static SegmentedJournal createJournal(
      final Path directory, final int indexDensity, final int segmentSizeInMB) {
    final UnsafeBuffer data = new UnsafeBuffer("test".getBytes(StandardCharsets.UTF_8));
    final BufferWriter recordDataWriter = new DirectBufferWriter().wrap(data);

    final var journal =
        SegmentedJournal.builder()
            .withDirectory(directory.resolve("data").toFile())
            .withMaxSegmentSize((int) DataSize.ofMegabytes(segmentSizeInMB).toBytes())
            .withMetaStore(new MockJournalMetastore())
            .withJournalIndexDensity(indexDensity)
            .build();
    journal.append(0, recordDataWriter);
    for (int i = 1; journal.getFirstSegment() == journal.getLastSegment(); i++) {
      journal.append(i, recordDataWriter);
    }
    System.out.printf(
        "Wrote %d entries. Iterations will seek to %d%n",
        journal.getLastIndex(), journal.getLastSegment().index() - 1);
    return journal;
  }

  @State(Scope.Benchmark)
  public static class JournalWithIndex {
    @Param({"100", "1000000000"})
    public int indexDensity;

    @Param({"128", "512"})
    public int segmentSizeInMB;

    Path directory;
    JournalReader reader;
    private SegmentedJournal journal;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      directory = Files.createTempDirectory("zeebe-journal-jmh");

      journal = createJournal(directory, indexDensity, segmentSizeInMB);

      reader = journal.openReader();
    }
  }
}
