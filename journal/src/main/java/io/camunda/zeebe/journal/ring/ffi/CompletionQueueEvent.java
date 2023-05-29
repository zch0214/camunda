/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring.ffi;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.StructLayout;

/**
 * A mapping of the `struct io_uring_cqe` to Java, allowing us to directly access its fields. There
 * are functions to access the user data separately from just a pointer, but unfortunately no way to
 * access the flags and/or result.
 *
 * @param pointer pointer to the underlying memory for this event
 */
public record CompletionQueueEvent(Pointer pointer) {
  private static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());

  /**
   * Returns a 64-bit value provided as user data. May be either a pointer or a plain value. If it
   * is a pointer, you can use {@link Pointer#wrap(Runtime, long)} to address it.
   */
  public long userData() {
    return LAYOUT.user_data.get(pointer);
  }

  /**
   * Returns the result of the operation that was submitted, e.g. the number of bytes read, the
   * number of bytes written, etc.
   */
  public int result() {
    return LAYOUT.res.get(pointer);
  }

  /** Returns the io_uring flags that were set on the operation, e.g. IOSQE_IO_LINK */
  public long flags() {
    return LAYOUT.flags.get(pointer);
  }

  @Override
  public String toString() {
    return "CompletionQueueEntry{ref=[address="
        + pointer.address()
        + ", size="
        + pointer.size()
        + "], user_data="
        + userData()
        + ", res="
        + res()
        + ", flags="
        + flags()
        + "}";
  }

  public int res() {
    return LAYOUT.res.get(pointer);
  }

  private static final class Layout extends StructLayout {
    private final Unsigned64 user_data = new Unsigned64();
    private final Signed32 res = new Signed32();
    private final Unsigned32 flags = new Unsigned32();

    public Layout(final Runtime runtime) {
      super(runtime);
    }
  }
}
