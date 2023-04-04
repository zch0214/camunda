/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring;

import jnr.ffi.Memory;
import jnr.ffi.NativeType;
import jnr.ffi.Runtime;
import jnr.ffi.StructLayout;

public final class SubmissionQueue {
  static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());

  private final Layout layout;
  private final jnr.ffi.Pointer pointer;

  public SubmissionQueue() {
    this(LAYOUT, Memory.allocateDirect(LAYOUT.getRuntime(), LAYOUT.size()));
  }

  public SubmissionQueue(final Layout layout, final jnr.ffi.Pointer pointer) {
    this.layout = layout;
    this.pointer = pointer;
  }

  public jnr.ffi.Pointer pointer() {
    return pointer;
  }

  private static final class Layout extends StructLayout {
    private final Pointer khead = new Pointer();
    private final Pointer ktail = new Pointer();
    private final Pointer kring_mask = new Pointer();
    private final Pointer kring_entries = new Pointer();
    private final Pointer kflags = new Pointer();
    private final Pointer kdropped = new Pointer();
    private final Pointer array = new Pointer();
    private final Pointer sqes = new Pointer();

    private final Unsigned32 sqe_head = new Unsigned32();
    private final Unsigned32 sqe_tail = new Unsigned32();

    private final size_t ring_sz = new size_t();
    private final Pointer ring_ptr = new Pointer();

    private final Unsigned32 ring_mask = new Unsigned32();
    private final Unsigned32 ring_entries = new Unsigned32();

    private final Padding pad = new Padding(NativeType.UINT, 2);

    public Layout(final Runtime runtime) {
      super(runtime);
    }
  }
}
