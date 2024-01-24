/*
 * Copyright 2015-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.raft.protocol;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.atomix.raft.cluster.RaftMember;
import java.util.Collection;
import java.util.Objects;

/**
 * Configuration installation request.
 *
 * <p>Configuration requests are special requests that aid in installing committed configurations to
 * passive and reserve members of the cluster. Prior to the start of replication from an active
 * member to a passive or reserve member, the active member must update the passive/reserve member's
 * configuration to ensure it is in the expected state.
 */
public class ForceConfigureRequest extends AbstractRaftRequest {

  private final long term;
  private final long index;
  private final long timestamp;
  private final Collection<RaftMember> members;

  public ForceConfigureRequest(
      final long term,
      final long index,
      final long timestamp,
      final Collection<RaftMember> newMembers) {
    this.term = term;
    this.index = index;
    this.timestamp = timestamp;
    members = newMembers;
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, index, timestamp, members);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ForceConfigureRequest that = (ForceConfigureRequest) o;
    return term == that.term
        && index == that.index
        && timestamp == that.timestamp
        && Objects.equals(members, that.members);
  }

  @Override
  public String toString() {
    return "ForceConfigureRequest{"
        + "term="
        + term
        + '\''
        + ", index="
        + index
        + ", timestamp="
        + timestamp
        + ", newMembers="
        + members
        + '}';
  }

  /**
   * Returns a new configuration request builder.
   *
   * @return A new configuration request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the configuration timestamp.
   *
   * @return The configuration timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the configuration members.
   *
   * @return The configuration members.
   */
  public Collection<RaftMember> newMembers() {
    return members;
  }

  /** Heartbeat request builder. */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, ForceConfigureRequest> {

    private long term;
    private long index;
    private long timestamp;
    private Collection<RaftMember> newMembers;

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withTerm(final long term) {
      checkArgument(term >= 0, "term must be positive");
      this.term = term;
      return this;
    }

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    public Builder withIndex(final long index) {
      checkArgument(index >= 0, "index must be positive");
      this.index = index;
      return this;
    }

    /**
     * Sets the request timestamp.
     *
     * @param timestamp The request timestamp.
     * @return The request builder.
     */
    public Builder withTime(final long timestamp) {
      checkArgument(timestamp > 0, "timestamp must be positive");
      this.timestamp = timestamp;
      return this;
    }

    /**
     * Sets the request members.
     *
     * @param newMembers The request members.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    public Builder withNewMembers(final Collection<RaftMember> newMembers) {
      this.newMembers = checkNotNull(newMembers, "members cannot be null");
      return this;
    }

    /**
     * @throws IllegalStateException if member is null
     */
    @Override
    public ForceConfigureRequest build() {
      validate();
      return new ForceConfigureRequest(term, index, timestamp, newMembers);
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(term >= 0, "term must be positive");
      checkArgument(index >= 0, "index must be positive");
      checkArgument(timestamp > 0, "timestamp must be positive");
      checkNotNull(newMembers, "newMembers cannot be null");
    }
  }
}
