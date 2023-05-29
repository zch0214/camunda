/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring;

import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.StructLayout;

public final class CompletionQueueEntry {
  private static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());
  private final jnr.ffi.Pointer pointer;

  public CompletionQueueEntry() {
    this(Memory.allocateDirect(LAYOUT.getRuntime(), LAYOUT.size()));
  }

  public CompletionQueueEntry(final Pointer pointer) {
    this.pointer = pointer;
  }

  public jnr.ffi.Pointer pointer() {
    return pointer;
  }

  public long user_data64() {
    return LAYOUT.user_data.get(pointer);
  }

  public int result() {
    return LAYOUT.res.get(pointer);
  }

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
        + user_data64()
        + ", res="
        + res()
        + ", flags="
        + flags()
        + "}";
  }

  public int res() {
    return LAYOUT.res.get(pointer);
  }

  @SuppressWarnings("unused")
  private static final class Layout extends StructLayout {
    private final Unsigned64 user_data = new Unsigned64();
    private final Signed32 res = new Signed32();
    private final Unsigned32 flags = new Unsigned32();

    public Layout(final Runtime runtime) {
      super(runtime);
    }
  }
}
