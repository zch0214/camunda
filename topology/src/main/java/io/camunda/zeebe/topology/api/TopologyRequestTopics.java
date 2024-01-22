/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.api;

public enum TopologyRequestTopics {
  ADD_MEMBER("topology-member-add"),
  REMOVE_MEMBER("topology-member-remove"),
  JOIN_PARTITION("topology-partition-join"),
  LEAVE_PARTITION("topology-partition-leave"),
  REASSIGN_PARTITIONS("topology-partition-reassign"),
  SCALE_MEMBERS("topology-member-scale"),
  QUERY_TOPOLOGY("topology-query"),
  CANCEL_CHANGE("topology-change-cancel"),
  FORCE_OVERWRITE_TOPOLOGY("topology-force-overwrite");

  private final String topic;

  TopologyRequestTopics(final String topic) {
    this.topic = topic;
  }

  public String topic() {
    return topic;
  }
}
