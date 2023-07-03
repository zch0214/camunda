/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.stream.job;

import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.msgpack.property.ArrayProperty;
import io.camunda.zeebe.msgpack.property.LongProperty;
import io.camunda.zeebe.msgpack.property.StringProperty;
import io.camunda.zeebe.msgpack.value.StringValue;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;

public class JobActivationPropertiesImpl extends UnpackedObject implements JobActivationProperties {
  private final StringProperty workerProp = new StringProperty("worker", "");
  private final LongProperty timeoutProp = new LongProperty("timeout", -1);
  private final ArrayProperty<StringValue> fetchVariablesProp =
      new ArrayProperty<>("variables", new StringValue());

  public void JobActivationProperties() {
    declareProperty(workerProp).declareProperty(timeoutProp).declareProperty(fetchVariablesProp);
  }

  public JobActivationProperties setWorker(
      final DirectBuffer worker, final int offset, final int length) {
    workerProp.setValue(worker, offset, length);
    return this;
  }

  public JobActivationProperties setTimeout(final long val) {
    timeoutProp.setValue(val);
    return this;
  }

  public JobActivationProperties setFetchVariables(final List<StringValue> variables) {
    fetchVariablesProp.reset();
    variables.forEach(variable -> fetchVariablesProp.add().wrap(variable));
    return this;
  }

  @Override
  public DirectBuffer worker() {
    return workerProp.getValue();
  }

  @Override
  public Collection<DirectBuffer> fetchVariables() {
    return fetchVariablesProp.stream().map(StringValue::getValue).collect(Collectors.toList());
  }

  @Override
  public long timeout() {
    return timeoutProp.getValue();
  }
}
