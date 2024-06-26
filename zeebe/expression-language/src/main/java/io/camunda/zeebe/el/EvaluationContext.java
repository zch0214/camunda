/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.el;

import java.util.Optional;
import org.agrona.DirectBuffer;

/** The context for evaluating an expression. */
public interface EvaluationContext {
  /**
   * Returns the value of the variable with the given name.
   *
   * @param variableName the name of the variable
   * @return the variable value as MessagePack encoded buffer, or {@code null} if the variable is
   *     not present
   */
  DirectBuffer getVariable(String variableName);

  /**
   * Combines two evaluation contexts. The combined evaluation context will first search for the
   * variable in {@code this} evaluation context. If the variable is not found, it will attempt the
   * lookup in {@code secondaryEvaluationContext}.
   *
   * @param secondaryEvaluationContext secondary evaluation context; this will be used to lookup
   *     variables which are not found in {@code this} evaluation context
   * @return combined evaluation context
   */
  default EvaluationContext combine(final EvaluationContext secondaryEvaluationContext) {
    return variable ->
        Optional.ofNullable(getVariable(variable))
            .orElseGet(() -> secondaryEvaluationContext.getVariable(variable));
  }
}
