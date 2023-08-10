/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.clustering.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEvent.Type;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClusterTopologyGossipHandlerImpl implements ClusterTopologyGossipHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClusterTopologyGossipHandlerImpl.class);

  private static final String CLUSTER_TOPOLOGY_PROPERTY_KEY = "cluster-topology";
  private final ClusterMembershipService membershipService;

  ClusterTopologyGossipHandlerImpl(final ClusterMembershipService membershipService) {
    this.membershipService = membershipService;
  }

  @Override
  public void gossip(final ClusterTopology clusterTopology) {
    try {
      membershipService
          .getLocalMember()
          .properties()
          // TODO: This has to be versioned, to allow future backward incompatible changes such as
          // changing serialization format
          .setProperty(CLUSTER_TOPOLOGY_PROPERTY_KEY, clusterTopology.encodeAsString());
    } catch (final JsonProcessingException e) {
      LOGGER.warn("Failed to propagate cluster topology {}", clusterTopology, e);
    }
  }

  @Override
  public void registerListener(final Consumer<ClusterTopology> clusterTopologyConsumer) {
    membershipService.addListener(new ClusterTopologyUpdateListener(clusterTopologyConsumer));
  }

  private record ClusterTopologyUpdateListener(Consumer<ClusterTopology> clusterTopologyConsumer)
      implements ClusterMembershipEventListener {

    @Override
    public boolean isRelevant(final ClusterMembershipEvent event) {
      return event.type() == Type.MEMBER_ADDED || event.type() == Type.METADATA_CHANGED;
    }

    @Override
    public void event(final ClusterMembershipEvent event) {
      final var encodedClusterTopology =
          event.subject().properties().getProperty(CLUSTER_TOPOLOGY_PROPERTY_KEY);
      if (encodedClusterTopology != null) {
        try {
          final var clusterTopology = ClusterTopology.decodeFromString(encodedClusterTopology);
          clusterTopologyConsumer.accept(clusterTopology);
        } catch (final JsonProcessingException e) {
          LOGGER.warn("Failed to read cluster topology from membership event {}", event, e);
        }
      }
    }
  }
}
