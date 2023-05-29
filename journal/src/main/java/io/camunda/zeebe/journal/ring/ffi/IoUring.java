/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring.ffi;

import jnr.ffi.Memory;
import jnr.ffi.NativeType;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.StructLayout;

/**
 * Java representation of the following the io_uring struct. Having a mapping here allows us to
 * introspect the ring and its two queues. We can of course use it without the mapping, but then we
 * cannot easily check what is the state of the ring.
 *
 * <p>Alternatively you can just allocate the appropriate amount of memory and use a pointer with
 * that instead.
 */
public record IoUring(Pointer pointer) {
  private static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());

  public IoUring() {
    this(Memory.allocateDirect(LAYOUT.getRuntime(), LAYOUT.size()));
  }

  @Override
  public String toString() {
    return "IoUring{"
        + "address="
        + pointer.address()
        + ", ring_fd="
        + LAYOUT.ring_fd.get(pointer)
        + ", flags="
        + LAYOUT.flags.get(pointer)
        + ", submittedEntries="
        + LAYOUT.sq.ring_entries.get(pointer)
        + ", completedEntries="
        + LAYOUT.cq.ring_entries.get(pointer)
        + ", "
        + "}";
  }

  @SuppressWarnings("unused")
  private static final class Layout extends StructLayout {
    private final SubmissionQueueLayout sq = inner(new SubmissionQueueLayout(getRuntime()));
    private final CompletionQueueLayout cq = inner(new CompletionQueueLayout(getRuntime()));
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
  }

  @SuppressWarnings("unused")
  private static final class SubmissionQueueLayout extends StructLayout {
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

    private SubmissionQueueLayout(final Runtime runtime) {
      super(runtime);
    }
  }

  @SuppressWarnings("unused")
  private static final class CompletionQueueLayout extends StructLayout {
    private final Pointer khead = new Pointer();
    private final Pointer ktail = new Pointer();
    private final Pointer kring_mask = new Pointer();
    private final Pointer kring_entries = new Pointer();
    private final Pointer kflags = new Pointer();
    private final Pointer koverflow = new Pointer();
    private final Pointer cqes = new Pointer();
    private final size_t ring_sz = new size_t();
    private final Pointer ring_ptr = new Pointer();
    private final Unsigned32 ring_mask = new Unsigned32();
    private final Unsigned32 ring_entries = new Unsigned32();
    private final Padding pad = new Padding(NativeType.UINT, 2);

    private CompletionQueueLayout(final Runtime runtime) {
      super(runtime);
    }
  }
}
