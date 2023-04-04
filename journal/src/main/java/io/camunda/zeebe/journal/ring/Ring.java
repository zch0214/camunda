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

/** Java representation of the following the io_uring struct. */
public final class Ring {
  private static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());

  private final Layout layout;
  private final jnr.ffi.Pointer pointer;

  public Ring() {
    this(LAYOUT, Memory.allocateDirect(LAYOUT.getRuntime(), LAYOUT.size()));
  }

  public Ring(final Layout layout, final jnr.ffi.Pointer pointer) {
    this.layout = layout;
    this.pointer = pointer;
  }

  public jnr.ffi.Pointer pointer() {
    return pointer;
  }

  private static final class Layout extends StructLayout {
    private final StructField sq = new StructField(SubmissionQueue.LAYOUT);
    private final StructField cq = new StructField(CompletionQueue.LAYOUT);
    private final Unsigned32 flags = new Unsigned32();
    private final Signed32 ring_fd = new Signed32();
    private final Unsigned32 features = new Unsigned32();
    private final Signed32 enter_ring_fd = new Signed32();
    private final Unsigned8 int_flags = new Unsigned8();
    private final Padding pad = new Padding(NativeType.UCHAR, 3);
    private final Padding pad2 = new Padding(NativeType.UINT, 1);

    public Layout(final Runtime runtime) {
      super(runtime);
    }

    private final class StructField extends AbstractField {
      private StructField(final StructLayout layout) {
        super(layout.size(), layout.alignment());
      }
    }
  }
}
