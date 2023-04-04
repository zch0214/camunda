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
import jnr.ffi.byref.PointerByReference;

public final class CompletionQueueEntry {
  private static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());
  private final jnr.ffi.Pointer pointer;
  private final PointerByReference reference;

  public CompletionQueueEntry() {
    this(Memory.allocateDirect(Runtime.getSystemRuntime(), LAYOUT.size()));
  }

  public CompletionQueueEntry(final Pointer pointer) {
    this(pointer, new PointerByReference(pointer));
  }

  public CompletionQueueEntry(final Pointer pointer, final PointerByReference reference) {
    this.pointer = pointer;
    this.reference = reference;
  }

  public jnr.ffi.Pointer pointer() {
    return pointer;
  }

  public PointerByReference reference() {
    return reference;
  }

  public Pointer user_data() {
    return LAYOUT.user_data.get(pointer);
  }

  public int res() {
    return LAYOUT.res.get(pointer);
  }

  @SuppressWarnings("unused")
  private static final class Layout extends StructLayout {
    private final Pointer user_data = new Pointer();
    private final Signed32 res = new Signed32();
    private final Unsigned32 flags = new Unsigned32();

    public Layout(final Runtime runtime) {
      super(runtime);
    }
  }
}
