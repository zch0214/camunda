/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.virtual;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.agrona.LangUtil;

/**
 * A simpler variant of {@link java.util.concurrent.DelayQueue}. It behaves roughly the same, but
 * does not support blocking behavior as defined in {@link java.util.concurrent.BlockingQueue}.
 *
 * <p>The queue is bounded (by default to {@link Integer#MAX_VALUE}), thread safe, and can be
 * "closed", which will cause no further items to be added to the queue.
 *
 * <h2>ToDos
 *
 * <ul>
 *   <li>Use a lock-free SkipList - see these papers for more: <a
 *       href="http://www.non-blocking.com/download/SunT03_PQueue_TR.pdf" /> and <a
 *       href="https://people.csail.mit.edu/shanir/publications/Priority_Queues.pdf" /> and <a
 *       href="http://www.cse.yorku.ca/~ruppert/papers/lfll.pdf" />
 * </ul>
 *
 * @param <E> the type of entries in the queue
 */
public class TaskQueue<E extends Delayed> extends AbstractQueue<E> implements Queue<E> {
  private final Lock lock = new ReentrantLock();
  private final PriorityQueue<E> queue = new PriorityQueue<>();
  private final int bound;

  public TaskQueue() {
    this(Integer.MAX_VALUE);
  }

  public TaskQueue(final int bound) {
    this.bound = bound;
  }

  @Override
  public Iterator<E> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    lock();

    try {
      return queue.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean remove(final Object o) {
    lock();

    try {
      return queue.remove(o);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean offer(final E e) {
    Objects.requireNonNull(e, "must specify an entry to add");
    lock();

    try {
      if (queue.size() + 1 >= bound) {
        return false;
      }

      queue.offer(e);
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public E poll() {
    lock();

    try {
      final E head = queue.peek();
      if (head == null || head.getDelay(TimeUnit.NANOSECONDS) > 0) {
        return null;
      }

      return queue.poll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public E peek() {
    lock();

    try {
      return queue.peek();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    lock();

    try {
      queue.clear();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean addAll(final Collection<? extends E> c) {
    Objects.requireNonNull(c, "must specify a collection to add");
    if (c == this) {
      throw new IllegalArgumentException("Cannot this collection to itself");
    }

    lock();

    try {
      boolean modified = false;
      for (final E item : c) {
        if (queue.size() < bound) {
          queue.add(item);
          modified = true;
        }
      }

      return modified;
    } finally {
      lock.unlock();
    }
  }

  public void drainTo(final Collection<? super E> remaining) {
    lock();

    try {
      remaining.addAll(queue);
      queue.clear();
    } finally {
      lock.unlock();
    }
  }

  private void lock() {
    try {
      lock.lockInterruptibly();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LangUtil.rethrowUnchecked(e);
    }
  }
}
