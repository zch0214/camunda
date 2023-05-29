/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring;

import io.camunda.zeebe.util.Loggers;
import java.util.Map;
import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;
import jnr.ffi.Pointer;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.types.u_int32_t;
import jnr.ffi.types.u_int64_t;

@SuppressWarnings({"checkstyle:methodname", "unused", "CheckStyle"})
public interface LibURing {
  int io_uring_queue_init(final @In int entries, final @Out Pointer ring, final @In int flags);

  @Out
  Pointer io_uring_get_sqe(final @In Pointer ring);

  void io_uring_prep_write(
      final @In Pointer sqe,
      final @In int fd,
      final @In Pointer buf,
      final @In @u_int32_t long nbytes,
      final @In @u_int64_t long offset);

  int io_uring_submit(final @In Pointer ring);

  int io_uring_wait_cqe(final @In Pointer ring, final @Out PointerByReference cqe_ptr);

  int io_uring_wait_cqe_nr(
      final @In Pointer ring,
      final @Out PointerByReference cqe_ptr,
      final @In @u_int32_t long wait_nr);

  void io_uring_cqe_seen(final @In Pointer ring, final @In Pointer cqe);

  void io_uring_prep_fsync(
      final @In Pointer sqe, final @In int fd, final @In @u_int32_t long flags);

  void io_uring_sqe_set_data(final @In Pointer sqe, final @In Pointer user_data);

  void io_uring_sqe_set_data64(final @In Pointer sqe, final @In @u_int64_t long user_data);

  @Out
  Pointer io_uring_cqe_get_data(final @In Pointer cqe);

  @u_int64_t
  long io_uring_cqe_get_data64(final @In Pointer cqe);

  void io_uring_sqe_set_flags(final @In Pointer sqe, final @In @u_int32_t long flags);

  void io_uring_prep_close(final @In Pointer sqe, final @In int fd);

  void io_uring_queue_exit(final @In Pointer ring);

  /**
   * Returns an instance of liburing. Currently only checks known locations, nothing pre-packaged.
   *
   * <p>If it fails to bind to the C library, it will return a {@link Invalid} instance which throws
   * {@link UnsupportedOperationException} on every call.
   *
   * @return an instance of this library
   */
  static LibURing ofNativeLibrary() {
    try {
      return LibraryLoader.loadLibrary(
          LibURing.class, Map.of(LibraryOption.LoadNow, true), "uring-ffi");
    } catch (final UnsatisfiedLinkError e) {
      Loggers.FILE_LOGGER.warn(
          "Failed to load system liburing library; any native calls will not be available", e);
      return new Invalid();
    }
  }

  @SuppressWarnings("CheckStyle")
  final class Invalid implements LibURing {
    @Override
    public int io_uring_queue_init(final int entries, final Pointer ring, final int flags) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Pointer io_uring_get_sqe(final Pointer ring) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_prep_write(
        final Pointer sqe, final int fd, final Pointer buf, final long nbytes, final long offset) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int io_uring_submit(final Pointer ring) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int io_uring_wait_cqe(final Pointer ring, final PointerByReference cqe) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_cqe_seen(final Pointer ring, final Pointer cqe) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_prep_fsync(final Pointer sqe, final int fd, final long flags) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_sqe_set_data(final Pointer sqe, final Pointer user_data) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_sqe_set_data64(final Pointer sqe, final long user_data) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Pointer io_uring_cqe_get_data(final Pointer cqe) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long io_uring_cqe_get_data64(final Pointer cqe) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_sqe_set_flags(final Pointer sqe, final long flags) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int io_uring_wait_cqe_nr(final Pointer ring, final PointerByReference cqe_ptr,
        final long wait_nr) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_prep_close(final Pointer sqe, final int fd) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void io_uring_queue_exit(final Pointer ring) {
      throw new UnsupportedOperationException();
    }
  }
}
