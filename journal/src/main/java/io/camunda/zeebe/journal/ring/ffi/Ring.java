/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.ring.ffi;

import jnr.ffi.Pointer;

/** Convenience class adapter for a low level {@code struct io_uring} instance. */
public final class Ring {
  private final Pointer ringPointer;
  private final LibURing nativeLibrary;

  public Ring() {
    this(LibURing.allocateRingStruct(), LibURing.ofNativeLibrary());
  }

  public Ring(final Pointer ringPointer, final LibURing nativeLibrary) {
    this.ringPointer = ringPointer;
    this.nativeLibrary = nativeLibrary;
  }

  public long submittedEntries() {
    return nativeLibrary.io_uring_sq_ready(ringPointer);
  }

  public long completedEntries() {
    return nativeLibrary.io_uring_cq_ready(ringPointer);
  }

  public Pointer ringPointer() {
    return ringPointer;
  }

  public LibURing library() {
    return nativeLibrary;
  }

  public Pointer newSqe() {
    return nativeLibrary.io_uring_get_sqe(ringPointer);
  }
}
