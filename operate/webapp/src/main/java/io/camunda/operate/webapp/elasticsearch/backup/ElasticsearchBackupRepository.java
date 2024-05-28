/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.operate.webapp.elasticsearch.backup;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.snapshots.SnapshotState.FAILED;
import static org.elasticsearch.snapshots.SnapshotState.INCOMPATIBLE;
import static org.elasticsearch.snapshots.SnapshotState.IN_PROGRESS;
import static org.elasticsearch.snapshots.SnapshotState.PARTIAL;
import static org.elasticsearch.snapshots.SnapshotState.SUCCESS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.operate.conditions.ElasticsearchCondition;
import io.camunda.operate.exceptions.OperateElasticsearchConnectionException;
import io.camunda.operate.exceptions.OperateRuntimeException;
import io.camunda.operate.property.OperateProperties;
import io.camunda.operate.util.ThreadUtil;
import io.camunda.operate.webapp.api.v1.exceptions.ResourceNotFoundException;
import io.camunda.operate.webapp.backup.BackupRepository;
import io.camunda.operate.webapp.backup.BackupService;
import io.camunda.operate.webapp.backup.Metadata;
import io.camunda.operate.webapp.management.dto.BackupStateDto;
import io.camunda.operate.webapp.management.dto.GetBackupStateResponseDetailDto;
import io.camunda.operate.webapp.management.dto.GetBackupStateResponseDto;
import io.camunda.operate.webapp.rest.exception.InvalidRequestException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.transport.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

@Conditional(ElasticsearchCondition.class)
@Component
public class ElasticsearchBackupRepository implements BackupRepository {
  public static final String SNAPSHOT_MISSING_EXCEPTION_TYPE = "type=snapshot_missing_exception";
  private static final String REPOSITORY_MISSING_EXCEPTION_TYPE =
      "type=repository_missing_exception";
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchBackupRepository.class);
  private final RestHighLevelClient esClient;
  private final ObjectMapper objectMapper;
  private final OperateProperties operateProperties;

  public ElasticsearchBackupRepository(
      final RestHighLevelClient esClient,
      final ObjectMapper objectMapper,
      final OperateProperties operateProperties) {
    this.esClient = esClient;
    this.objectMapper = objectMapper;
    this.operateProperties = operateProperties;
  }

  @Override
  public void deleteSnapshot(final String repositoryName, final String snapshotName) {
    final DeleteSnapshotRequest request = new DeleteSnapshotRequest(repositoryName);
    request.snapshots(snapshotName);
    esClient.snapshot().deleteAsync(request, RequestOptions.DEFAULT, getDeleteListener());
  }

  @Override
  public void validateRepositoryExists(final String repositoryName) {
    final GetRepositoriesRequest getRepositoriesRequest =
        new GetRepositoriesRequest().repositories(new String[] {repositoryName});
    try {
      esClient.snapshot().getRepository(getRepositoriesRequest, RequestOptions.DEFAULT);
    } catch (final IOException | TransportException ex) {
      final String reason =
          String.format(
              "Encountered an error connecting to Elasticsearch while retrieving repository with name [%s].",
              repositoryName);
      throw new OperateElasticsearchConnectionException(reason, ex);
    } catch (final Exception e) {
      if (isRepositoryMissingException(e)) {
        final String reason =
            String.format("No repository with name [%s] could be found.", repositoryName);
        throw new OperateRuntimeException(reason);
      }
      final String reason =
          String.format(
              "Exception occurred when validating existence of repository with name [%s].",
              repositoryName);
      throw new OperateRuntimeException(reason, e);
    }
  }

  @Override
  public void validateNoDuplicateBackupId(final String repositoryName, final Long backupId) {
    final GetSnapshotsRequest snapshotsStatusRequest =
        new GetSnapshotsRequest()
            .repository(repositoryName)
            .snapshots(new String[] {Metadata.buildSnapshotNamePrefix(backupId) + "*"});
    final GetSnapshotsResponse response;
    try {
      response = esClient.snapshot().get(snapshotsStatusRequest, RequestOptions.DEFAULT);
    } catch (final IOException | TransportException ex) {
      final String reason =
          String.format(
              "Encountered an error connecting to Elasticsearch while searching for duplicate backup. Repository name: [%s].",
              repositoryName);
      throw new OperateElasticsearchConnectionException(reason, ex);
    } catch (final Exception e) {
      if (isSnapshotMissingException(e)) {
        // no snapshot with given backupID exists
        return;
      }
      final String reason =
          String.format(
              "Exception occurred when validating whether backup with ID [%s] already exists.",
              backupId);
      throw new OperateRuntimeException(reason, e);
    }
    if (!response.getSnapshots().isEmpty()) {
      final String reason =
          String.format(
              "A backup with ID [%s] already exists. Found snapshots: [%s]",
              backupId,
              response.getSnapshots().stream()
                  .map(snapshotInfo -> snapshotInfo.snapshotId().toString())
                  .collect(joining(", ")));
      throw new InvalidRequestException(reason);
    }
  }

  @Override
  public GetBackupStateResponseDto getBackupState(
      final String repositoryName, final Long backupId) {
    final List<SnapshotInfo> snapshots = findSnapshots(repositoryName, backupId);
    return getBackupResponse(backupId, snapshots);
  }

  @Override
  public List<GetBackupStateResponseDto> getBackups(final String repositoryName) {
    final GetSnapshotsRequest snapshotsStatusRequest =
        new GetSnapshotsRequest()
            .repository(repositoryName)
            .snapshots(new String[] {Metadata.SNAPSHOT_NAME_PREFIX + "*"})
            // it looks like sorting as well as size/offset are not working, need to sort
            // additionally before return
            .sort(GetSnapshotsRequest.SortBy.START_TIME)
            .order(SortOrder.DESC);
    final GetSnapshotsResponse response;
    try {
      response = esClient.snapshot().get(snapshotsStatusRequest, RequestOptions.DEFAULT);
      final List<SnapshotInfo> snapshots =
          response.getSnapshots().stream()
              .sorted(Comparator.comparing(SnapshotInfo::startTime).reversed())
              .collect(toList());

      final LinkedHashMap<Long, List<SnapshotInfo>> groupedSnapshotInfos =
          snapshots.stream()
              .collect(
                  groupingBy(
                      si -> {
                        final Metadata metadata =
                            objectMapper.convertValue(si.userMetadata(), Metadata.class);
                        Long backupId = metadata.getBackupId();
                        // backward compatibility with v. 8.1
                        if (backupId == null) {
                          backupId =
                              Metadata.extractBackupIdFromSnapshotName(si.snapshotId().getName());
                        }
                        return backupId;
                      },
                      LinkedHashMap::new,
                      toList()));

      return groupedSnapshotInfos.entrySet().stream()
          .map(entry -> getBackupResponse(entry.getKey(), entry.getValue()))
          .collect(toList());
    } catch (final IOException | TransportException ex) {
      final String reason =
          String.format(
              "Encountered an error connecting to Elasticsearch while searching for snapshots. Repository name: [%s].",
              repositoryName);
      throw new OperateElasticsearchConnectionException(reason, ex);
    } catch (final Exception e) {
      if (isRepositoryMissingException(e)) {
        final String reason =
            String.format("No repository with name [%s] could be found.", repositoryName);
        throw new OperateRuntimeException(reason);
      }
      if (isSnapshotMissingException(e)) {
        // no snapshots exist
        return new ArrayList<>();
      }
      final String reason =
          String.format("Exception occurred when searching for backups: %s", e.getMessage());
      throw new OperateRuntimeException(reason, e);
    }
  }

  @Override
  public void executeSnapshotting(
      final BackupService.SnapshotRequest snapshotRequest,
      final Runnable onSuccess,
      final Runnable onFailure) {
    final var request =
        new CreateSnapshotRequest()
            .repository(snapshotRequest.repositoryName())
            .snapshot(snapshotRequest.snapshotName())
            .indices(snapshotRequest.indices())
            // ignoreUnavailable = false - indices defined by their exact name MUST be present
            // allowNoIndices = true - indices defined by wildcards, e.g. archived, MIGHT BE absent
            .indicesOptions(IndicesOptions.fromOptions(false, true, true, true))
            .userMetadata(
                objectMapper.convertValue(snapshotRequest.metadata(), new TypeReference<>() {}))
            .featureStates(new String[] {"none"})
            .waitForCompletion(true);
    final var listener = new CreateSnapshotListener(snapshotRequest, onSuccess, onFailure);

    esClient.snapshot().createAsync(request, RequestOptions.DEFAULT, listener);
  }

  private ActionListener<AcknowledgedResponse> getDeleteListener() {
    return new ActionListener<>() {
      @Override
      public void onResponse(final AcknowledgedResponse response) {
        LOGGER.debug(
            "Delete snapshot was acknowledged by Elasticsearch node: {}",
            response.isAcknowledged());
      }

      @Override
      public void onFailure(final Exception e) {
        if (isSnapshotMissingException(e)) {
          // no snapshot with given backupID exists, this is fine, log warning
          LOGGER.warn("No snapshot found for snapshot deletion: {} ", e.getMessage());
        } else {
          LOGGER.error("Exception occurred while deleting the snapshot: {}", e.getMessage(), e);
        }
      }
    };
  }

  private boolean isSnapshotMissingException(final Exception e) {
    return e instanceof ElasticsearchStatusException
        && ((ElasticsearchStatusException) e)
            .getDetailedMessage()
            .contains(SNAPSHOT_MISSING_EXCEPTION_TYPE);
  }

  private boolean isRepositoryMissingException(final Exception e) {
    return e instanceof ElasticsearchStatusException
        && ((ElasticsearchStatusException) e)
            .getDetailedMessage()
            .contains(REPOSITORY_MISSING_EXCEPTION_TYPE);
  }

  private List<SnapshotInfo> findSnapshots(final String repositoryName, final Long backupId) {
    final GetSnapshotsRequest snapshotsStatusRequest =
        new GetSnapshotsRequest()
            .repository(repositoryName)
            .snapshots(new String[] {Metadata.buildSnapshotNamePrefix(backupId) + "*"});
    final GetSnapshotsResponse response;
    try {
      response = esClient.snapshot().get(snapshotsStatusRequest, RequestOptions.DEFAULT);
      return response.getSnapshots();
    } catch (final IOException | TransportException ex) {
      final String reason =
          String.format(
              "Encountered an error connecting to Elasticsearch while searching for snapshots. Repository name: [%s].",
              repositoryName);
      throw new OperateElasticsearchConnectionException(reason, ex);
    } catch (final Exception e) {
      if (isSnapshotMissingException(e)) {
        // no snapshot with given backupID exists
        throw new ResourceNotFoundException(
            String.format("No backup with id [%s] found.", backupId));
      }
      if (isRepositoryMissingException(e)) {
        final String reason =
            String.format("No repository with name [%s] could be found.", repositoryName);
        throw new OperateRuntimeException(reason);
      }
      final String reason =
          String.format("Exception occurred when searching for backup with ID [%s].", backupId);
      throw new OperateRuntimeException(reason, e);
    }
  }

  private GetBackupStateResponseDto getBackupResponse(
      final Long backupId, final List<SnapshotInfo> snapshots) {
    final GetBackupStateResponseDto response = new GetBackupStateResponseDto(backupId);
    final var firstSnapshot = snapshots.get(0);
    final Metadata metadata =
        objectMapper.convertValue(firstSnapshot.userMetadata(), Metadata.class);
    final Integer expectedSnapshotsCount = metadata.getPartCount();
    final long startTimeInMilliseconds = firstSnapshot.startTime();

    if (snapshots.size() == expectedSnapshotsCount
        && snapshots.stream().map(SnapshotInfo::state).allMatch(SUCCESS::equals)) {
      response.setState(BackupStateDto.COMPLETED);
    } else if (snapshots.stream()
        .map(SnapshotInfo::state)
        .anyMatch(s -> FAILED.equals(s) || PARTIAL.equals(s))) {
      response.setState(BackupStateDto.FAILED);
    } else if (snapshots.stream().map(SnapshotInfo::state).anyMatch(INCOMPATIBLE::equals)) {
      response.setState(BackupStateDto.INCOMPATIBLE);
    } else if (snapshots.stream().map(SnapshotInfo::state).anyMatch(IN_PROGRESS::equals)) {
      response.setState(BackupStateDto.IN_PROGRESS);
    } else if (snapshots.size() < expectedSnapshotsCount) {
      if (isIncompleteCheckTimedOut(operateProperties, startTimeInMilliseconds)) {
        response.setState(BackupStateDto.INCOMPLETE);
      } else {
        response.setState(BackupStateDto.IN_PROGRESS);
      }
    } else {
      response.setState(BackupStateDto.FAILED);
    }
    final List<GetBackupStateResponseDetailDto> details = new ArrayList<>();
    for (final SnapshotInfo snapshot : snapshots) {
      final GetBackupStateResponseDetailDto detail = new GetBackupStateResponseDetailDto();
      detail.setSnapshotName(snapshot.snapshotId().getName());
      detail.setStartTime(
          OffsetDateTime.ofInstant(
              Instant.ofEpochMilli(snapshot.startTime()), ZoneId.systemDefault()));
      if (snapshot.shardFailures() != null) {
        detail.setFailures(
            snapshot.shardFailures().stream()
                .map(SnapshotShardFailure::toString)
                .toArray(String[]::new));
      }
      detail.setState(snapshot.state().name());
      details.add(detail);
    }
    response.setDetails(details);
    if (response.getState().equals(BackupStateDto.FAILED)) {
      String failureReason = null;
      final String failedSnapshots =
          snapshots.stream()
              .filter(s -> s.state().equals(FAILED))
              .map(s -> s.snapshotId().getName())
              .collect(Collectors.joining(", "));
      if (!failedSnapshots.isEmpty()) {
        failureReason =
            String.format("There were failures with the following snapshots: %s", failedSnapshots);
      } else {
        final String partialSnapshot =
            snapshots.stream()
                .filter(s -> s.state().equals(PARTIAL))
                .map(s -> s.snapshotId().getName())
                .collect(Collectors.joining(", "));
        if (!partialSnapshot.isEmpty()) {
          failureReason = String.format("Some of the snapshots are partial: %s", partialSnapshot);
        } else if (snapshots.size() > expectedSnapshotsCount) {
          failureReason = "More snapshots found than expected.";
        }
      }
      if (failureReason != null) {
        response.setFailureReason(failureReason);
      }
    }
    return response;
  }

  /** CreateSnapshotListener */
  private class CreateSnapshotListener implements ActionListener<CreateSnapshotResponse> {

    private final BackupService.SnapshotRequest snapshotRequest;
    private final long backupId;
    private final Runnable onSuccess;
    private final Runnable onFailure;

    public CreateSnapshotListener(
        final BackupService.SnapshotRequest snapshotRequest,
        final Runnable onSuccess,
        final Runnable onFailure) {
      this.snapshotRequest = snapshotRequest;
      backupId = Metadata.extractBackupIdFromSnapshotName(snapshotRequest.snapshotName());
      this.onSuccess = onSuccess;
      this.onFailure = onFailure;
    }

    @Override
    public void onResponse(final CreateSnapshotResponse response) {
      handleSnapshotReceived(response.getSnapshotInfo());
    }

    @Override
    public void onFailure(final Exception ex) {
      if (ex instanceof SocketTimeoutException) {
        // This is thrown even if the backup is still running
        LOGGER.warn(
            "Timeout while creating snapshot [{}] for backup id [{}]. Need to keep waiting with polling...",
            snapshotRequest.snapshotName(),
            backupId);
        // Keep waiting
        while (true) {
          final List<SnapshotInfo> snapshotInfos =
              findSnapshots(snapshotRequest.repositoryName(), backupId);
          final SnapshotInfo currentSnapshot =
              snapshotInfos.stream()
                  .filter(
                      x -> Objects.equals(x.snapshotId().getName(), snapshotRequest.snapshotName()))
                  .findFirst()
                  .orElse(null);
          if (currentSnapshot == null) {
            LOGGER.error(
                "Expected (but not found) snapshot [{}] for backupId [{}].",
                snapshotRequest.snapshotName(),
                backupId);
            // No need to continue
            onFailure.run();
            break;
          }
          if (currentSnapshot.state() == IN_PROGRESS) {
            ThreadUtil.sleepFor(100);
          } else {
            handleSnapshotReceived(currentSnapshot);
            break;
          }
        }
      } else {
        LOGGER.error(
            "Exception while creating snapshot [{}] for backup id [{}].",
            snapshotRequest.snapshotName(),
            backupId,
            ex);
        // No need to continue
        onFailure.run();
      }
    }

    private void handleSnapshotReceived(final SnapshotInfo snapshotInfo) {
      if (snapshotInfo.state() == SUCCESS) {
        LOGGER.info("Snapshot done: {}", snapshotInfo.snapshotId());
        onSuccess.run();
      } else if (snapshotInfo.state() == FAILED) {
        LOGGER.error(
            "Snapshot taking failed for {}, reason {}",
            snapshotInfo.snapshotId(),
            snapshotInfo.reason());
        // No need to continue
        onFailure.run();
      } else {
        LOGGER.warn(
            "Snapshot state is {} for snapshot {}",
            snapshotInfo.state(),
            snapshotInfo.snapshotId());
        onSuccess.run();
      }
    }
  }
}
