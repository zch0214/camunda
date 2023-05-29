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
import jnr.ffi.Union;
import jnr.ffi.byref.PointerByReference;

public class SubmissionQueueEntry {
  private static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());
  private final jnr.ffi.Pointer pointer;
  private final PointerByReference reference;

  public SubmissionQueueEntry() {
    this(Memory.allocateDirect(LAYOUT.getRuntime(), LAYOUT.size()));
  }

  public SubmissionQueueEntry(final Pointer pointer) {
    this(pointer, new PointerByReference(pointer));
  }

  public SubmissionQueueEntry(final Pointer pointer, final PointerByReference reference) {
    this.pointer = pointer;
    this.reference = reference;
  }

  public jnr.ffi.Pointer pointer() {
    return pointer;
  }

  public PointerByReference reference() {
    return reference;
  }

  public long len() {
    return LAYOUT.len.get(pointer);
  }

  public short flags() {
    return LAYOUT.flags.get(pointer);
  }

  public long user_data() {
    return LAYOUT.user_data.get(pointer);
  }

  public SubmissionQueueEntry user_data(final long data) {
    LAYOUT.user_data.set(pointer, data);
    return this;
  }

  private static final class Layout extends StructLayout {
    private final Unsigned8 opcode = new Unsigned8();
    private final Unsigned8 flags = new Unsigned8();
    private final Unsigned16 ioprio = new Unsigned16();
    private final Signed32 fd = new Signed32();
    private final Unsigned64 fileUnion = new Unsigned64();
    private final Unsigned64 spliceUnion = new Unsigned64();
    //    private final OffsetUnion offsetUnion = new OffsetUnion(getRuntime());
    //    private final SpliceUnion spliceUnion = new SpliceUnion(getRuntime());
    private final Unsigned32 len = new Unsigned32();
    private final Unsigned32 flagsUnion = new Unsigned32();
    //    private final FlagsUnion flagsUnion = new FlagsUnion(getRuntime());
    private final Unsigned64 user_data = new Unsigned64();
    private final Unsigned64[] __pad2 = new Unsigned64[3];

    private Layout(final Runtime runtime) {
      super(runtime);
    }

    private static final class OffsetUnion extends Union {
      private final Unsigned64 off = new Unsigned64();
      private final Pointer addr2 = new Pointer();

      private OffsetUnion(final Runtime runtime) {
        super(runtime);
      }
    }

    private static final class SpliceUnion extends Union {
      private final Pointer addr = new Pointer();
      private final Unsigned64 splice_off_in = new Unsigned64();

      private SpliceUnion(final Runtime runtime) {
        super(runtime);
      }
    }

    private static final class FlagsUnion extends Union {
      private final Signed32 rw_flags = new Signed32();
      private final Unsigned32 fsync_flags = new Unsigned32();
      private final Unsigned16 poll_events = new Unsigned16();
      private final Unsigned32 sync_range_flags = new Unsigned32();
      private final Unsigned32 msg_flags = new Unsigned32();
      private final Unsigned32 timeout_flags = new Unsigned32();
      private final Unsigned32 accept_flags = new Unsigned32();
      private final Unsigned32 cancel_flags = new Unsigned32();
      private final Unsigned32 open_flags = new Unsigned32();
      private final Unsigned32 statx_flags = new Unsigned32();
      private final Unsigned32 fadvise_advice = new Unsigned32();
      private final Unsigned32 splice_flags = new Unsigned32();

      private FlagsUnion(final Runtime runtime) {
        super(runtime);
      }
    }
  }
}
