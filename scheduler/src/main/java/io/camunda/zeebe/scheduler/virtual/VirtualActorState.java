/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.virtual;

public enum VirtualActorState {
  // The actor is not busy and can be scheduled
  IDLE,
  // The actor is currently busy executing tasks
  BUSY,
  // The actor is currently suspended; tasks can be submitted, but nothing will be executed
  SUSPENDED,
  // The actor is stopped; tasks cannot be submitted anymore, and nothing will be executed anymore
  STOPPED;
}
