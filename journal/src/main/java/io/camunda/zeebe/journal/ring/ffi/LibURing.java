/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring.ffi;

import java.util.Map;
import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.byref.PointerByReference;
import jnr.ffi.types.u_int32_t;
import jnr.ffi.types.u_int64_t;

/**
 * Maps the functions provided by `liburing` to FFI function calls.
 *
 * <p>For a full list of functions, see <a
 * href="https://manpages.debian.org/unstable/liburing-dev/index.html">the manpages</a>
 *
 * <p>And white paper: <a href="https://kernel.dk/io_uring.pdf">here</a>
 */
@SuppressWarnings({"checkstyle:methodname", "unused", "CheckStyle"})
public interface LibURing {

  /**
   * Allocates a new `struct io_uring` on the heap. Only useful if you want to use a ring that you
   * will not directly introspect. If you want to do that, use the {@link IoUring} and {@link
   * IoUring#pointer()}.
   *
   * <p>NOTE: to get the size of the struct, you can run the following:
   *
   * <pre>@{code
   * gcc -I/usr/include -I/usr/include/liburing -luring -o out sizeof.c && ./out && rm out
   * }</pre>
   *
   * @return a pointer to an io_uring struct
   */
  static Pointer allocateRingStruct() {
    final var size = 216;
    final var runtime = Runtime.getSystemRuntime();
    return Memory.allocateDirect(runtime, size);
  }

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

  @u_int32_t
  long io_uring_cq_ready(final @In Pointer ring);

  @u_int32_t
  long io_uring_sq_ready(final @In Pointer ring);

  /**
   * Returns an instance of liburing. Currently only checks known locations, nothing pre-packaged.
   *
   * @return an instance of this library
   * @throws UnsatisfiedLinkError if the library is not found
   */
  static LibURing ofNativeLibrary() {
    return LibraryLoader.loadLibrary(
        LibURing.class, Map.of(LibraryOption.LoadNow, true), "uring-ffi");
  }
}
