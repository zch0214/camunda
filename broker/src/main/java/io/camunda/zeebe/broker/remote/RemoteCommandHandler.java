/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.remote;

import io.camunda.zeebe.broker.remote.RemoteCommandReceiver.TransformedRequest;
import org.agrona.DirectBuffer;

@FunctionalInterface
public interface RemoteCommandHandler {

  TransformedRequest apply(DirectBuffer buffer, int offset);
}
