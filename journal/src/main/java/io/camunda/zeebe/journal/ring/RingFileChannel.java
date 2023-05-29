/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring;

import io.camunda.zeebe.journal.ring.ffi.Ring;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.IntConsumer;
import jnr.constants.platform.Errno;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import org.agrona.collections.Long2ObjectHashMap;

public final class RingFileChannel implements AsynchronousByteChannel {
  private final Long2ObjectHashMap<IntConsumer> callbacks = new Long2ObjectHashMap<>();

  private final int fd;
  private final Ring ring;
  private final Path path;

  private int idCounter = 0;
  private long offset = 0;

  public RingFileChannel(final Path path, final int fd, final Ring ring) {
    this.path = path;
    this.fd = fd;
    this.ring = ring;
  }

  @Override
  public <A> void read(
      final ByteBuffer dst,
      final A attachment,
      final CompletionHandler<Integer, ? super A> handler) {}

  @Override
  public Future<Integer> read(final ByteBuffer dst) {
    return null;
  }

  @Override
  public <A> void write(
      final ByteBuffer src,
      final A attachment,
      final CompletionHandler<Integer, ? super A> handler) {
    final long id = ((long) fd << 32) | idCounter++;
    final var sqe = ring.newSqe();

    // keep track of the offset here
    ring.library()
        .io_uring_prep_write(
            sqe, fd, Pointer.wrap(sqe.getRuntime(), src.slice()), src.remaining(), 0);
    ring.library().io_uring_sqe_set_data64(sqe, id);
    final var result = ring.library().io_uring_submit(ring.ringPointer());
    if (result < 0) {
      final var error = mapError(result, path);
      handler.failed(error, attachment);
      return;
    }

    callbacks.put(id, new IOCallback<>(attachment, handler));
  }

  @Override
  public Future<Integer> write(final ByteBuffer src) {
    final var future = new CompletableFuture<Integer>();
    write(src, null, new CompletableFutureHandler<>(future));
    return future;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public boolean isOpen() {
    return false;
  }

  public static RingFileChannel open(final Path path, final Ring ring, final OpenFlags... flags)
      throws IOException {
    final var posix = POSIXFactory.getNativePOSIX();
    final var mode =
        Arrays.stream(flags).mapToInt(OpenFlags::intValue).reduce(0, (v1, v2) -> v1 | v2);
    final var fd = posix.open(path.toAbsolutePath().toString(), mode, 420);
    if (fd == -1) {
      throw mapError(path);
    }

    return new RingFileChannel(path, fd, ring);
  }

  private static IOException mapError(final Path path) {
    final POSIX posix = POSIXFactory.getNativePOSIX();
    return mapError(posix.errno(), path);
  }

  private static IOException mapError(final int errno, final Path path) {
    final POSIX posix = POSIXFactory.getNativePOSIX();
    final var message = posix.strerror(errno);
    final var namedError = Errno.valueOf(errno);
    final var exceptionMessage =
        "Failed on ring backed file at [%s]; failed with code [%s]: %s"
            .formatted(path, namedError, message);

    if (namedError == Errno.EEXIST) {
      return new FileAlreadyExistsException(exceptionMessage);
    }

    return new IOException(exceptionMessage);
  }

  private record CompletableFutureHandler<Integer>(CompletableFuture<Integer> future)
      implements CompletionHandler<Integer, Void> {

    @Override
    public void completed(final Integer result, final Void attachment) {
      future.complete(result);
    }

    @Override
    public void failed(final Throwable exc, final Void attachment) {
      future.completeExceptionally(exc);
    }
  }

  private final class IOCallback<T> implements IntConsumer {
    private final T attachment;
    private final CompletionHandler<Integer, T> handler;

    private IOCallback(final T attachment, final CompletionHandler<Integer, T> handler) {
      this.attachment = attachment;
      this.handler = handler;
    }

    @Override
    public void accept(final int value) {
      if (value >= 0) {
        handler.completed(value, attachment);
        return;
      }

      final var error = mapError(path);
      handler.failed(error, attachment);
    }
  }
}
