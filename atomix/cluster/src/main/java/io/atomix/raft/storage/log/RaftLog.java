/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.storage.log;

import static io.camunda.zeebe.journal.file.SegmentedJournal.ASQN_IGNORE;

import io.atomix.raft.protocol.PersistedRaftRecord;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.storage.serializer.RaftEntrySBESerializer;
import io.atomix.raft.storage.serializer.RaftEntrySerializer;
import io.camunda.zeebe.journal.Journal;
import io.camunda.zeebe.journal.JournalRecord;
import io.camunda.zeebe.util.Environment;
import io.camunda.zeebe.util.buffer.BufferWriter;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableBoolean;
import org.slf4j.LoggerFactory;

/** Raft log. */
public final class RaftLog implements Closeable {
  private static final long FLUSH_BYTES_THRESHOLD =
      new Environment().getLong("ZEEBE_BROKER_EXPERIMENTAL_RAFT_FLUSHBYTES").orElse(512 * 1024L);

  private final Journal journal;
  private final RaftEntrySerializer serializer = new RaftEntrySBESerializer();
  private final boolean flushExplicitly;

  private IndexedRaftLogEntry lastAppendedEntry;
  private volatile long commitIndex;
  private final AtomicLong unflushedBytes = new AtomicLong();
  private final long flushBytesThreshold;

  RaftLog(final Journal journal, final boolean flushExplicitly) {
    this(journal, flushExplicitly, FLUSH_BYTES_THRESHOLD);
  }

  RaftLog(final Journal journal, final boolean flushExplicitly, final long flushBytesThreshold) {
    this.journal = journal;
    this.flushExplicitly = flushExplicitly;
    this.flushBytesThreshold = flushBytesThreshold;
    LoggerFactory.getLogger(getClass())
        .info("Starting RaftLog with flush bytes threshold {}", flushBytesThreshold);
  }

  /**
   * Returns a new Raft log builder.
   *
   * @return A new Raft log builder.
   */
  public static RaftLogBuilder builder() {
    return new RaftLogBuilder();
  }

  /**
   * Opens the reader that can read both committed and uncommitted entries.
   *
   * @return the reader
   */
  public RaftLogReader openUncommittedReader() {
    return new RaftLogUncommittedReader(journal.openReader());
  }

  /**
   * Opens the reader that can only read committed entries.
   *
   * @return the reader
   */
  public RaftLogReader openCommittedReader() {
    return new RaftLogCommittedReader(this, new RaftLogUncommittedReader(journal.openReader()));
  }

  public boolean isOpen() {
    return journal.isOpen();
  }

  /**
   * Compacts the journal up to the given index.
   *
   * <p>The semantics of compaction are not specified by this interface.
   *
   * @param index The index up to which to compact the journal.
   */
  public void deleteUntil(final long index) {
    journal.deleteUntil(index);
  }

  /**
   * Returns the Raft log commit index.
   *
   * @return The Raft log commit index.
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   */
  public void setCommitIndex(final long index) {
    commitIndex = index;
  }

  public boolean shouldFlushExplicitly() {
    return flushExplicitly;
  }

  public long getFirstIndex() {
    return journal.getFirstIndex();
  }

  public long getLastIndex() {
    return journal.getLastIndex();
  }

  public IndexedRaftLogEntry getLastEntry() {
    if (lastAppendedEntry == null) {
      readLastEntry();
    }

    return lastAppendedEntry;
  }

  private void readLastEntry() {
    try (final var reader = openUncommittedReader()) {
      reader.seekToLast();
      if (reader.hasNext()) {
        lastAppendedEntry = reader.next();
      }
    }
  }

  public boolean isEmpty() {
    return journal.isEmpty();
  }

  public IndexedRaftLogEntry append(final RaftLogEntry entry) {
    final BufferWriter serializableEntry = entry.entry().toSerializable(entry.term(), serializer);
    final JournalRecord journalRecord =
        journal.append(entry.getLowestAsqn().orElse(ASQN_IGNORE), serializableEntry);

    unflushedBytes.addAndGet(serializableEntry.getLength());
    lastAppendedEntry = new IndexedRaftLogEntryImpl(entry.term(), entry.entry(), journalRecord);
    return lastAppendedEntry;
  }

  public IndexedRaftLogEntry append(final PersistedRaftRecord entry) {
    journal.append(entry);

    final RaftLogEntry raftEntry = serializer.readRaftLogEntry(entry.data());
    lastAppendedEntry = new IndexedRaftLogEntryImpl(entry.term(), raftEntry.entry(), entry);
    unflushedBytes.addAndGet(entry.approximateSize());
    return lastAppendedEntry;
  }

  public void reset(final long index) {
    journal.reset(index);
    lastAppendedEntry = null;
    unflushedBytes.set(flushBytesThreshold); // force next flush to go through
  }

  public void deleteAfter(final long index) {
    if (index < commitIndex) {
      throw new IllegalStateException(
          String.format(
              "Expected to delete index after %d, but it is lower than the commit index %d. Deleting committed entries can lead to inconsistencies and is prohibited.",
              index, commitIndex));
    }
    journal.deleteAfter(index);
    unflushedBytes.set(flushBytesThreshold); // force next flush to go through
    lastAppendedEntry = null;
  }

  public boolean flush() {
    if (!flushExplicitly) {
      return false;
    }

    final var shouldFlush = new MutableBoolean(false);
    unflushedBytes.getAndUpdate(
        bytesCount -> {
          if (bytesCount >= flushBytesThreshold) {
            shouldFlush.set(true);
            return 0;
          }

          return bytesCount;
        });

    if (shouldFlush.get()) {
      journal.flush();
      return true;
    }

    return false;
  }

  @Override
  public void close() {
    CloseHelper.close(journal);
  }

  @Override
  public String toString() {
    return "RaftLog{"
        + "journal="
        + journal
        + ", serializer="
        + serializer
        + ", flushExplicitly="
        + flushExplicitly
        + ", lastAppendedEntry="
        + lastAppendedEntry
        + ", commitIndex="
        + commitIndex
        + ", flushBytes="
        + flushBytesThreshold
        + '}';
  }
}
