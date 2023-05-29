/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal;

import com.kenai.jffi.MemoryIO;
import com.sun.nio.file.ExtendedOpenOption;
import io.camunda.zeebe.journal.ring.CompletionQueueEntry;
import io.camunda.zeebe.journal.ring.IoEntry;
import io.camunda.zeebe.journal.ring.IoEntry.Type;
import io.camunda.zeebe.journal.ring.LibURing;
import io.camunda.zeebe.journal.ring.Ring;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import org.agrona.IoUtil;
import org.junit.jupiter.api.Test;
import org.springframework.util.StopWatch;

public class IoTest {

  @Test
  void shouldCreateRing() throws IOException {
    final var liburing = LibURing.ofNativeLibrary();
    final var ring = new Ring();
    var result = liburing.io_uring_queue_init(2, ring.pointer(), 0);
    final POSIX posix = POSIXFactory.getNativePOSIX();
    if (result < 0) {
      System.out.println("Failed to init ring: " + posix.strerror(-result));
      System.exit(1);
    }

    final var fd =
        posix.open(
            "/home/nicolas/tmp/ring.buf",
            OpenFlags.O_CREAT.intValue()
                | OpenFlags.O_RDWR.intValue()
                | OpenFlags.O_APPEND.intValue(),
            0644);
    final byte[] bytes = (LocalDateTime.now() + " - Hello Ring World!\n").getBytes();
    final var buffer =
        ByteBuffer.allocateDirect(bytes.length).order(ByteOrder.nativeOrder()).put(bytes).rewind();

    // write and flush pipelined
    var sqePointer = liburing.io_uring_get_sqe(ring.pointer());
    liburing.io_uring_prep_write(
        sqePointer, fd, Pointer.wrap(sqePointer.getRuntime(), buffer), bytes.length, 0);
    liburing.io_uring_sqe_set_flags(sqePointer, /* IOSQE_IO_LINK */ 4);
    liburing.io_uring_sqe_set_data64(sqePointer, 1L);
    submit(liburing, ring);

    sqePointer = liburing.io_uring_get_sqe(ring.pointer());
    liburing.io_uring_prep_fsync(sqePointer, fd, 0);
    liburing.io_uring_sqe_set_flags(sqePointer, /* IOSQE_IO_LINK */ 4);
    liburing.io_uring_sqe_set_data64(sqePointer, 2L);
    submit(liburing, ring);

    waitForCompletion(liburing, ring);
    waitForCompletion(liburing, ring);

    sqePointer = liburing.io_uring_get_sqe(ring.pointer());
    liburing.io_uring_prep_close(sqePointer, fd);
    liburing.io_uring_sqe_set_flags(sqePointer, /* IOSQE_IO_LINK */ 4);
    liburing.io_uring_sqe_set_data64(sqePointer, 3L);
    submit(liburing, ring);
    waitForCompletion(liburing, ring);

    // close the ring
    liburing.io_uring_queue_exit(ring.pointer());

    final int a = 1;
  }

  private static void submit(final LibURing liburing, final Ring ring) {
    int result;
    result = liburing.io_uring_submit(ring.pointer());
    if (result < 0) {
      System.out.println(
          "Failed to submit entries to ring: " + POSIXFactory.getNativePOSIX().strerror(-result));
      System.exit(1);
    }
  }

  private static void waitForCompletion(final LibURing liburing, final Ring ring) {
    final var reference = new PointerByReference();
    final int result = liburing.io_uring_wait_cqe(ring.pointer(), reference);
    final var cqe = new CompletionQueueEntry(reference.getValue());
    if (result < 0) {
      System.out.println(
          "Failed to await completion on ring: " + POSIXFactory.getNativePOSIX().strerror(-result));
      System.exit(1);
    }

    if (cqe.res() < 0) {
      System.out.println(
          "Specific entry failed: " + POSIXFactory.getNativePOSIX().strerror(-cqe.res()));
      System.exit(1);
    }

    final var userData = liburing.io_uring_cqe_get_data64(cqe.pointer());
    liburing.io_uring_cqe_seen(ring.pointer(), cqe.pointer());
  }

  @Test
  void run() throws IOException {
    final var watch = new StopWatch("IoTest");
    final var buffer = ByteBuffer.allocateDirect(1024 * 1024);
    final File file = new File("/home/nicolas/tmp/test.log");
    final long blockSize =
        file.toPath().getFileSystem().getFileStores().iterator().next().getBlockSize();

    file.delete();

    final int fileSize = 1024 * 1024 * 1024;
    IoUtil.createEmptyFile(file, fileSize, true).close();

    try {
      try (final FileChannel channel =
          FileChannel.open(file.toPath(), StandardOpenOption.WRITE, ExtendedOpenOption.DIRECT)) {
        watch.start("directIOWrite");
        write(channel, fileSize, blockSize, buffer);
        watch.stop();
        System.out.println("Elapsed: " + watch.getLastTaskTimeMillis() + "ms");
      }

      try (final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
        watch.start("bufferedIOWrite");
        bufferedWrite(channel, fileSize, blockSize, buffer);
        watch.stop();
        System.out.println("Elapsed: " + watch.getLastTaskTimeMillis() + "ms");
      }
    } finally {
      System.out.println(watch.prettyPrint());
      file.delete();
    }
  }

  private void write(
      final FileChannel channel, final long size, final long blockSize, final ByteBuffer buffer)
      throws IOException {
    long offset = 0;
    while (offset < size) {
      channel.write(buffer.rewind(), offset);
      channel.force(true);
      offset += buffer.capacity();
    }
  }

  private void bufferedWrite(
      final FileChannel channel, final long size, final long blockSize, final ByteBuffer buffer)
      throws IOException {
    long offset = 0;
    while (offset < size) {
      channel.write(buffer.rewind(), offset);
      channel.force(true);
      offset += buffer.capacity();
    }
  }
}
