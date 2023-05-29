/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring;

import io.camunda.zeebe.journal.ring.ffi.CompletionQueueEvent;
import io.camunda.zeebe.journal.ring.ffi.IoUring;
import io.camunda.zeebe.journal.ring.ffi.LibURing;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import org.junit.jupiter.api.Test;

final class LibURingTest {

  @Test
  void shouldCreateRing() {
    final var liburing = LibURing.ofNativeLibrary();
    final var ring = new IoUring();
    final var ringPointer = ring.pointer(); // LibURing.allocateRingStruct();
    var result = liburing.io_uring_queue_init(2, ringPointer, 0);
    final POSIX posix = POSIXFactory.getNativePOSIX();
    if (result < 0) {
      System.out.println("Failed to init ring: " + posix.strerror(-result));
      System.exit(1);
    }

    final var fd = openFile(posix);
    ByteBuffer timestampedString;

    // write and flush pipelined
    var sqePointer = liburing.io_uring_get_sqe(ringPointer);
    timestampedString = createTimestampedString();
    liburing.io_uring_prep_write(
        sqePointer,
        fd,
        Pointer.wrap(sqePointer.getRuntime(), timestampedString),
        timestampedString.capacity(),
        0);
    liburing.io_uring_sqe_set_flags(sqePointer, /* IOSQE_IO_LINK */ 4);
    liburing.io_uring_sqe_set_data64(sqePointer, 1L);
    submit(liburing, ringPointer);

    timestampedString = createTimestampedString();
    sqePointer = liburing.io_uring_get_sqe(ringPointer);
    liburing.io_uring_prep_write(
        sqePointer,
        fd,
        Pointer.wrap(sqePointer.getRuntime(), timestampedString),
        timestampedString.capacity(),
        0);
    liburing.io_uring_sqe_set_flags(sqePointer, /* IOSQE_IO_LINK */ 4);
    liburing.io_uring_sqe_set_data64(sqePointer, 1L);
    submit(liburing, ringPointer);

    sqePointer = liburing.io_uring_get_sqe(ringPointer);
    liburing.io_uring_prep_fsync(sqePointer, fd, 0);
    liburing.io_uring_sqe_set_flags(sqePointer, /* IOSQE_IO_LINK */ 4);
    liburing.io_uring_sqe_set_data64(sqePointer, 2L);
    submit(liburing, ringPointer);

    waitForCompletion(liburing, ringPointer);
    waitForCompletion(liburing, ringPointer);

    sqePointer = liburing.io_uring_get_sqe(ringPointer);
    liburing.io_uring_prep_close(sqePointer, fd);
    liburing.io_uring_sqe_set_flags(sqePointer, /* IOSQE_IO_LINK */ 4);
    liburing.io_uring_sqe_set_data64(sqePointer, 3L);
    submit(liburing, ringPointer);
    waitForCompletion(liburing, ringPointer);

    // close the ring
    liburing.io_uring_queue_exit(ringPointer);
  }

  private ByteBuffer createTimestampedString() {
    final byte[] bytes = (LocalDateTime.now() + " - Hello Ring World!\n").getBytes();
    return ByteBuffer.allocateDirect(bytes.length)
        .order(ByteOrder.nativeOrder())
        .put(bytes)
        .rewind();
  }

  private int openFile(final POSIX posix) {
    return posix.open(
        "/home/nicolas/tmp/ring.buf",
        OpenFlags.O_CREAT.intValue() | OpenFlags.O_RDWR.intValue() | OpenFlags.O_APPEND.intValue(),
        0644);
  }

  private static void submit(final LibURing liburing, final Pointer ringPointer) {
    int result;
    result = liburing.io_uring_submit(ringPointer);
    if (result < 0) {
      System.out.println(
          "Failed to submit entries to ringPointer: "
              + POSIXFactory.getNativePOSIX().strerror(-result));
      System.exit(1);
    }
  }

  private static void waitForCompletion(final LibURing liburing, final Pointer ringPointer) {
    final var reference = new PointerByReference();
    final int result = liburing.io_uring_wait_cqe(ringPointer, reference);
    final var cqe = new CompletionQueueEvent(reference.getValue());
    if (result < 0) {
      System.out.println(
          "Failed to await completion on rin: " + POSIXFactory.getNativePOSIX().strerror(-result));
      System.exit(1);
    }

    if (cqe.res() < 0) {
      System.out.println(
          "Specific entry failed: " + POSIXFactory.getNativePOSIX().strerror(-cqe.res()));
      System.exit(1);
    }

    final var userData = liburing.io_uring_cqe_get_data64(cqe.pointer());
    liburing.io_uring_cqe_seen(ringPointer, cqe.pointer());
  }
}
