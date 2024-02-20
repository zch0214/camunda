/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.client.impl.response;

import io.camunda.zeebe.client.api.command.ClientException;
import io.camunda.zeebe.client.api.response.PartitionBrokerHealth;
import io.camunda.zeebe.client.api.response.PartitionBrokerRole;
import io.camunda.zeebe.client.api.response.PartitionInfo;
import io.camunda.zeebe.gateway.protocol.rest.Partition;
import io.camunda.zeebe.gateway.protocol.rest.Partition.HealthEnum;
import io.camunda.zeebe.gateway.protocol.rest.Partition.RoleEnum;
import java.util.Arrays;
import java.util.Objects;

public class PartitionInfoImpl implements PartitionInfo {

  private final int partitionId;
  private final PartitionBrokerRole role;
  private final PartitionBrokerHealth health;

  public PartitionInfoImpl(final Partition partition) {
    Objects.requireNonNull(partition, "must specify a partition");

    partitionId = partition.getPartitionId();
    role = mapRole(partition.getRole());
    health = mapHealth(partition.getHealth());
  }

  private PartitionBrokerRole mapRole(final RoleEnum role) {
    switch (role) {
      case LEADER:
        return PartitionBrokerRole.LEADER;
      case FOLLOWER:
        return PartitionBrokerRole.FOLLOWER;
      case INACTIVE:
        return PartitionBrokerRole.INACTIVE;
      default:
        throw new ClientException(
            String.format(
                "Expected partition role to be one of %s, but was %s",
                Arrays.toString(PartitionBrokerRole.values()), role));
    }
  }

  private PartitionBrokerHealth mapHealth(final HealthEnum health) {
    switch (health) {
      case HEALTHY:
        return PartitionBrokerHealth.HEALTHY;
      case UNHEALTHY:
        return PartitionBrokerHealth.UNHEALTHY;
      case DEAD:
        return PartitionBrokerHealth.DEAD;
      default:
        throw new ClientException(
            String.format(
                "Expected partition health to be one of %s, but was %s",
                Arrays.toString(PartitionBrokerHealth.values()), health));
    }
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public PartitionBrokerRole getRole() {
    return role;
  }

  @Override
  public boolean isLeader() {
    return role == PartitionBrokerRole.LEADER;
  }

  @Override
  public PartitionBrokerHealth getHealth() {
    return health;
  }

  @Override
  public String toString() {
    return "PartitionInfoImpl{"
        + "partitionId="
        + partitionId
        + ", role="
        + role
        + ", health="
        + health
        + '}';
  }
}
