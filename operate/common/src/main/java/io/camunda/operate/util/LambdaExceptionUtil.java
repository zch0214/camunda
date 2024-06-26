/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.operate.util;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Helper class to rethrow checked exceptions in lambda expressions. */
public final class LambdaExceptionUtil {

  /**
   * .forEach(rethrowConsumer(name -> System.out.println(Class.forName(name)))); or
   * .forEach(rethrowConsumer(ClassNameUtil::println));
   */
  public static <T, E extends Exception> Consumer<T> rethrowConsumer(
      ConsumerWithExceptions<T, E> consumer) throws E {
    return t -> {
      try {
        consumer.accept(t);
      } catch (Exception exception) {
        throwAsUnchecked(exception);
      }
    };
  }

  public static <T, U, E extends Exception> BiConsumer<T, U> rethrowBiConsumer(
      BiConsumerWithExceptions<T, U, E> biConsumer) throws E {
    return (t, u) -> {
      try {
        biConsumer.accept(t, u);
      } catch (Exception exception) {
        throwAsUnchecked(exception);
      }
    };
  }

  /** .map(rethrowFunction(name -> Class.forName(name))) or .map(rethrowFunction(Class::forName)) */
  public static <T, R, E extends Exception> Function<T, R> rethrowFunction(
      FunctionWithExceptions<T, R, E> function) throws E {
    return t -> {
      try {
        return function.apply(t);
      } catch (Exception exception) {
        throwAsUnchecked(exception);
        return null;
      }
    };
  }

  /** rethrowSupplier(() -> new StringJoiner(new String(new byte[]{77, 97, 114, 107}, "UTF-8"))), */
  public static <T, E extends Exception> Supplier<T> rethrowSupplier(
      SupplierWithExceptions<T, E> function) throws E {
    return () -> {
      try {
        return function.get();
      } catch (Exception exception) {
        throwAsUnchecked(exception);
        return null;
      }
    };
  }

  /** uncheck(() -> Class.forName("xxx")); */
  public static void uncheck(RunnableWithExceptions t) {
    try {
      t.run();
    } catch (Exception exception) {
      throwAsUnchecked(exception);
    }
  }

  /** uncheck(() -> Class.forName("xxx")); */
  public static <R, E extends Exception> R uncheck(SupplierWithExceptions<R, E> supplier) {
    try {
      return supplier.get();
    } catch (Exception exception) {
      throwAsUnchecked(exception);
      return null;
    }
  }

  /** uncheck(Class::forName, "xxx"); */
  public static <T, R, E extends Exception> R uncheck(
      FunctionWithExceptions<T, R, E> function, T t) {
    try {
      return function.apply(t);
    } catch (Exception exception) {
      throwAsUnchecked(exception);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E {
    throw (E) exception;
  }

  @FunctionalInterface
  public interface ConsumerWithExceptions<T, E extends Exception> {
    void accept(T t) throws E;
  }

  @FunctionalInterface
  public interface BiConsumerWithExceptions<T, U, E extends Exception> {
    void accept(T t, U u) throws E;
  }

  @FunctionalInterface
  public interface FunctionWithExceptions<T, R, E extends Exception> {
    R apply(T t) throws E;
  }

  @FunctionalInterface
  public interface SupplierWithExceptions<T, E extends Exception> {
    T get() throws E;
  }

  @FunctionalInterface
  public interface RunnableWithExceptions<E extends Exception> {
    void run() throws E;
  }
}
