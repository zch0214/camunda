/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring;

import com.kenai.jffi.MemoryIO;
import java.nio.ByteBuffer;
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.StructLayout;

public final class IoEntry {

  static final Layout LAYOUT = new Layout(Runtime.getSystemRuntime());

  private final jnr.ffi.Pointer pointer;

  public IoEntry() {
    this(Memory.allocateDirect(LAYOUT.getRuntime(), LAYOUT.size(), true));
  }

  public IoEntry(final jnr.ffi.Pointer pointer) {
    this.pointer = pointer;
  }

  public jnr.ffi.Pointer pointer() {
    return pointer;
  }

  public IoEntry fd(final int fd) {
    LAYOUT.fd.set(pointer, fd);
    return this;
  }

  public int fd() {
    return LAYOUT.fd.get(pointer);
  }

  private static final class Layout extends StructLayout {
    private final Signed32 fd = new Signed32();
    private final Signed8 type = new Signed8();
    private final Pointer buffer = new Pointer();

    public Layout(final Runtime runtime) {
      super(runtime);
    }
  }

  public enum Type {
    WRITE(0),
    UNKNOWN(-1);

    private final int value;

    Type(final int value) {
      this.value = value;
    }

    public static Type of(final int value) {
      for (final Type v : values()) {
        if (v.value == value) {
          return v;
        }
      }

      return UNKNOWN;
    }
  }
}
