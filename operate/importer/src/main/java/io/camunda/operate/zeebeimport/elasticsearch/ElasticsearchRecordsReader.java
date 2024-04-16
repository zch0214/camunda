/*
 * Copyright Camunda Services GmbH
 *
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING, OR DISTRIBUTING THE SOFTWARE (“USE”), YOU INDICATE YOUR ACCEPTANCE TO AND ARE ENTERING INTO A CONTRACT WITH, THE LICENSOR ON THE TERMS SET OUT IN THIS AGREEMENT. IF YOU DO NOT AGREE TO THESE TERMS, YOU MUST NOT USE THE SOFTWARE. IF YOU ARE RECEIVING THE SOFTWARE ON BEHALF OF A LEGAL ENTITY, YOU REPRESENT AND WARRANT THAT YOU HAVE THE ACTUAL AUTHORITY TO AGREE TO THE TERMS AND CONDITIONS OF THIS AGREEMENT ON BEHALF OF SUCH ENTITY.
 * “Licensee” means you, an individual, or the entity on whose behalf you receive the Software.
 *
 * Permission is hereby granted, free of charge, to the Licensee obtaining a copy of this Software and associated documentation files to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject in each case to the following conditions:
 * Condition 1: If the Licensee distributes the Software or any derivative works of the Software, the Licensee must attach this Agreement.
 * Condition 2: Without limiting other conditions in this Agreement, the grant of rights is solely for non-production use as defined below.
 * "Non-production use" means any use of the Software that is not directly related to creating products, services, or systems that generate revenue or other direct or indirect economic benefits.  Examples of permitted non-production use include personal use, educational use, research, and development. Examples of prohibited production use include, without limitation, use for commercial, for-profit, or publicly accessible systems or use for commercial or revenue-generating purposes.
 *
 * If the Licensee is in breach of the Conditions, this Agreement, including the rights granted under it, will automatically terminate with immediate effect.
 *
 * SUBJECT AS SET OUT BELOW, THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * NOTHING IN THIS AGREEMENT EXCLUDES OR RESTRICTS A PARTY’S LIABILITY FOR (A) DEATH OR PERSONAL INJURY CAUSED BY THAT PARTY’S NEGLIGENCE, (B) FRAUD, OR (C) ANY OTHER LIABILITY TO THE EXTENT THAT IT CANNOT BE LAWFULLY EXCLUDED OR RESTRICTED.
 */
package io.camunda.operate.zeebeimport.elasticsearch;

import static io.camunda.operate.Metrics.GAUGE_IMPORT_QUEUE_SIZE;
import static io.camunda.operate.Metrics.TAG_KEY_PARTITION;
import static io.camunda.operate.Metrics.TAG_KEY_TYPE;
import static io.camunda.operate.util.ElasticsearchUtil.*;
import static io.camunda.operate.util.ThreadUtil.sleepFor;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import io.camunda.operate.Metrics;
import io.camunda.operate.conditions.ElasticsearchCondition;
import io.camunda.operate.entities.HitEntity;
import io.camunda.operate.entities.meta.ImportPositionEntity;
import io.camunda.operate.exceptions.NoSuchIndexException;
import io.camunda.operate.exceptions.OperateRuntimeException;
import io.camunda.operate.property.OperateProperties;
import io.camunda.operate.schema.indices.ImportPositionIndex;
import io.camunda.operate.util.BackoffIdleStrategy;
import io.camunda.operate.util.NumberThrottleable;
import io.camunda.operate.zeebe.ImportValueType;
import io.camunda.operate.zeebeimport.*;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/**
 * Represents Zeebe data reader for one partition and one value type. After reading the data is also
 * schedules the jobs for import execution. Each reader can have its own backoff, so that we make a
 * pause in case there is no data currently for given partition and value type.
 */
@Conditional(ElasticsearchCondition.class)
@Component
@Scope(SCOPE_PROTOTYPE)
public class ElasticsearchRecordsReader implements RecordsReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchRecordsReader.class);

  /** Partition id. */
  private final int partitionId;

  /** Value type. */
  private final ImportValueType importValueType;

  /** The queue of executed tasks for execution. */
  private final BlockingQueue<Callable<Boolean>> importJobs;

  private final ReentrantLock schedulingImportJobLock;
  private NumberThrottleable batchSizeThrottle;

  /** The job that we are currently busy with. */
  private Callable<Boolean> active;

  private ImportJob pendingImportJob;
  private boolean ongoingRescheduling;

  private long maxPossibleSequence;

  private int countEmptyRuns;

  private BackoffIdleStrategy errorStrategy;

  @Autowired
  @Qualifier("importThreadPoolExecutor")
  private ThreadPoolTaskExecutor importExecutor;

  @Autowired
  @Qualifier("recordsReaderThreadPoolExecutor")
  private ThreadPoolTaskScheduler readersExecutor;

  @Autowired private ImportPositionHolder importPositionHolder;

  @Autowired private OperateProperties operateProperties;

  @Autowired
  @Qualifier("zeebeEsClient")
  private RestHighLevelClient zeebeEsClient;

  @Autowired private BeanFactory beanFactory;

  @Autowired private Metrics metrics;

  @Autowired(required = false)
  private List<ImportListener> importListeners;

  public ElasticsearchRecordsReader(
      final int partitionId, final ImportValueType importValueType, final int queueSize) {
    this.partitionId = partitionId;
    this.importValueType = importValueType;
    importJobs = new LinkedBlockingQueue<>(queueSize);
    schedulingImportJobLock = new ReentrantLock();
  }

  @PostConstruct
  private void postConstruct() {
    batchSizeThrottle =
        new NumberThrottleable.DivideNumberThrottle(
            operateProperties.getZeebeElasticsearch().getBatchSize());
    // 1st sequence of next partition - 1
    maxPossibleSequence = sequence(partitionId + 1, 0) - 1;
    countEmptyRuns = 0;
    errorStrategy =
        new BackoffIdleStrategy(operateProperties.getImporter().getReaderBackoff(), 1.2f, 10_000);
  }

  @Override
  public void run() {
    readAndScheduleNextBatch();
  }

  private void readAndScheduleNextBatch() {
    readAndScheduleNextBatch(true);
  }

  @Override
  public void readAndScheduleNextBatch(final boolean autoContinue) {
    final int readerBackoff = operateProperties.getImporter().getReaderBackoff();
    final boolean useOnlyPosition = operateProperties.getImporter().isUseOnlyPosition();
    try {
      metrics.registerGaugeQueueSize(
          GAUGE_IMPORT_QUEUE_SIZE,
          importJobs,
          TAG_KEY_PARTITION,
          String.valueOf(partitionId),
          TAG_KEY_TYPE,
          importValueType.name());
      final ImportBatch importBatch;
      final ImportPositionEntity latestPosition =
          importPositionHolder.getLatestScheduledPosition(
              importValueType.getAliasTemplate(), partitionId);
      if (!useOnlyPosition && latestPosition != null && latestPosition.getSequence() > 0) {
        LOGGER.debug("Use import for {} ( {} ) by sequence", importValueType.name(), partitionId);
        importBatch = readNextBatchBySequence(latestPosition.getSequence());
      } else {
        LOGGER.debug("Use import for {} ( {} ) by position", importValueType.name(), partitionId);
        importBatch = readNextBatchByPositionAndPartition(latestPosition.getPosition(), null);
      }
      Integer nextRunDelay = null;
      if (importBatch == null
          || importBatch.getHits() == null
          || importBatch.getHits().size() == 0) {
        nextRunDelay = readerBackoff;
      } else {
        final var importJob = createImportJob(latestPosition, importBatch);
        if (!scheduleImportJob(importJob, !autoContinue)) {
          // didn't succeed to schedule import job ->
          // reader gets scheduled once the queue has capacity
          // if autoContinue == false, the reader is controlled
          // outside the readers thread pool, in that case, the
          // one who is controlling the reader will/must trigger
          // another round to read the next batch.
          return;
        }
      }
      errorStrategy.reset();
      if (autoContinue) {
        rescheduleReader(nextRunDelay);
      }
    } catch (final NoSuchIndexException ex) {
      // if no index found, we back off current reader
      if (autoContinue) {
        rescheduleReader(readerBackoff);
      }
    } catch (final Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
      if (autoContinue) {
        errorStrategy.idle();
        rescheduleReader((int) errorStrategy.idleTime());
      }
    }
  }

  @Override
  public ImportBatch readNextBatchBySequence(final Long sequence, final Long lastSequence)
      throws NoSuchIndexException {
    final String aliasName =
        importValueType.getAliasName(operateProperties.getZeebeElasticsearch().getPrefix());
    final int batchSize = batchSizeThrottle.get();
    if (batchSize != batchSizeThrottle.getOriginal()) {
      LOGGER.warn(
          "Use new batch size {} (original {})", batchSize, batchSizeThrottle.getOriginal());
    }
    final long lessThanEqualsSequence;
    final int maxNumberOfHits;

    if (lastSequence != null && lastSequence > 0) {
      // in worst case all the records are duplicated
      maxNumberOfHits = (int) ((lastSequence - sequence) * 2);
      lessThanEqualsSequence = lastSequence;
      LOGGER.debug(
          "Import batch reread was called. Data type {}, partitionId {}, sequence {}, lastSequence {}, maxNumberOfHits {}.",
          importValueType,
          partitionId,
          sequence,
          lessThanEqualsSequence,
          maxNumberOfHits);
    } else {
      maxNumberOfHits = batchSize;
      if (countEmptyRuns == operateProperties.getImporter().getMaxEmptyRuns()) {
        lessThanEqualsSequence = maxPossibleSequence;
        countEmptyRuns = 0;
        LOGGER.debug(
            "Max empty runs reached. Data type {}, partitionId {}, sequence {}, lastSequence {}, maxNumberOfHits {}.",
            importValueType,
            partitionId,
            sequence,
            lessThanEqualsSequence,
            maxNumberOfHits);
      } else {
        lessThanEqualsSequence = sequence + batchSize;
      }
    }

    final SearchSourceBuilder searchSourceBuilder =
        new SearchSourceBuilder()
            .sort(ImportPositionIndex.SEQUENCE, SortOrder.ASC)
            .query(
                rangeQuery(ImportPositionIndex.SEQUENCE).gt(sequence).lte(lessThanEqualsSequence))
            .size(maxNumberOfHits >= QUERY_MAX_SIZE ? QUERY_MAX_SIZE : maxNumberOfHits);

    final SearchRequest searchRequest =
        new SearchRequest(aliasName)
            .source(searchSourceBuilder)
            .routing(String.valueOf(partitionId))
            .requestCache(false);

    try {
      final HitEntity[] hits =
          withTimerSearchHits(() -> read(searchRequest, maxNumberOfHits >= QUERY_MAX_SIZE));
      if (hits.length == 0) {
        countEmptyRuns++;
      } else {
        countEmptyRuns = 0;
      }
      return createImportBatch(hits);
    } catch (final ElasticsearchStatusException ex) {
      if (ex.getMessage().contains("no such index")) {
        throw new NoSuchIndexException();
      } else {
        final String message =
            String.format(
                "Exception occurred for alias [%s], while obtaining next Zeebe records batch: %s",
                aliasName, ex.getMessage());
        throw new OperateRuntimeException(message, ex);
      }
    } catch (final Exception e) {
      if (e.getMessage().contains("entity content is too long")) {
        LOGGER.info(
            "{}. Will decrease batch size for {}-{}",
            e.getMessage(),
            importValueType.name(),
            partitionId);
        batchSizeThrottle.throttle();
        return readNextBatchBySequence(sequence, lastSequence);
      } else {
        final String message =
            String.format(
                "Exception occurred for alias [%s], while obtaining next Zeebe records batch: %s",
                aliasName, e.getMessage());
        throw new OperateRuntimeException(message, e);
      }
    }
  }

  @Override
  public ImportBatch readNextBatchByPositionAndPartition(
      final long positionFrom, final Long positionTo) throws NoSuchIndexException {
    final String aliasName =
        importValueType.getAliasName(operateProperties.getZeebeElasticsearch().getPrefix());
    try {
      final SearchRequest searchRequest = createSearchQuery(aliasName, positionFrom, positionTo);
      final SearchResponse searchResponse =
          withTimer(() -> zeebeEsClient.search(searchRequest, RequestOptions.DEFAULT));
      checkForFailedShards(searchResponse);
      return createImportBatch(searchResponse);
    } catch (final ElasticsearchStatusException ex) {
      if (ex.getMessage().contains("no such index")) {
        throw new NoSuchIndexException();
      } else {
        final String message =
            String.format(
                "Exception occurred for alias [%s], while obtaining next Zeebe records batch: %s",
                aliasName, ex.getMessage());
        throw new OperateRuntimeException(message, ex);
      }
    } catch (final Exception e) {
      if (e.getMessage().contains("entity content is too long")) {
        batchSizeThrottle.throttle();
        return readNextBatchByPositionAndPartition(positionFrom, positionTo);
      } else {
        final String message =
            String.format(
                "Exception occurred for alias [%s], while obtaining next Zeebe records batch: %s",
                aliasName, e.getMessage());
        throw new OperateRuntimeException(message, e);
      }
    }
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public ImportValueType getImportValueType() {
    return importValueType;
  }

  @Override
  public BlockingQueue<Callable<Boolean>> getImportJobs() {
    return importJobs;
  }

  private ImportBatch readNextBatchBySequence(final Long sequence) throws NoSuchIndexException {
    return readNextBatchBySequence(sequence, null);
  }

  private HitEntity[] read(final SearchRequest searchRequest, final boolean scrollNeeded)
      throws IOException {
    String scrollId = null;
    try {
      final List<HitEntity> searchHits = new ArrayList<>();

      if (scrollNeeded) {
        searchRequest.scroll(TimeValue.timeValueMillis(SCROLL_KEEP_ALIVE_MS));
      }
      SearchResponse response = zeebeEsClient.search(searchRequest, requestOptions);
      checkForFailedShards(response);

      searchHits.addAll(
          Arrays.stream(response.getHits().getHits()).map(this::searchHitToOperateHit).toList());

      if (scrollNeeded) {
        scrollId = response.getScrollId();
        do {
          final SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
          scrollRequest.scroll(TimeValue.timeValueMillis(SCROLL_KEEP_ALIVE_MS));

          response = zeebeEsClient.scroll(scrollRequest, requestOptions);
          checkForFailedShards(response);

          scrollId = response.getScrollId();
          searchHits.addAll(
              Arrays.stream(response.getHits().getHits())
                  .map(this::searchHitToOperateHit)
                  .toList());
        } while (response.getHits().getHits().length != 0);
      }
      return searchHits.toArray(new HitEntity[0]);
    } finally {
      if (scrollId != null) {
        clearScroll(scrollId, zeebeEsClient);
      }
    }
  }

  private HitEntity searchHitToOperateHit(final SearchHit searchHit) {
    return new HitEntity()
        .setIndex(searchHit.getIndex())
        .setSourceAsString(searchHit.getSourceAsString());
  }

  private void rescheduleReader(final Integer readerDelay) {
    if (readerDelay != null) {
      readersExecutor.schedule(
          this, Date.from(OffsetDateTime.now().plus(readerDelay, ChronoUnit.MILLIS).toInstant()));
    } else {
      readersExecutor.submit(this);
    }
  }

  private ImportJob createImportJob(
      final ImportPositionEntity latestPosition, final ImportBatch importBatch) {
    return beanFactory.getBean(ImportJob.class, importBatch, latestPosition);
  }

  private boolean scheduleImportJob(final ImportJob job, final boolean skipPendingJob) {
    if (tryToScheduleImportJob(job, skipPendingJob)) {
      importJobScheduledSucceeded(job);
      return true;
    }
    return false;
  }

  private void importJobScheduledSucceeded(final ImportJob job) {
    metrics
        .getTimer(
            Metrics.TIMER_NAME_IMPORT_JOB_SCHEDULED_TIME,
            Metrics.TAG_KEY_TYPE,
            importValueType.name(),
            Metrics.TAG_KEY_PARTITION,
            String.valueOf(partitionId))
        .record(Duration.between(job.getCreationTime(), OffsetDateTime.now()));

    final var batch = job.getImportBatch();
    batch.setScheduledTime(OffsetDateTime.now());

    notifyImportListenersAsScheduled(batch);
    job.recordLatestScheduledPosition();
  }

  private void notifyImportListenersAsScheduled(final ImportBatch importBatch) {
    if (importListeners != null) {
      importListeners.forEach(listener -> listener.scheduled(importBatch));
    }
  }

  private void checkForFailedShards(final SearchResponse searchResponse) {
    if (searchResponse.getFailedShards() > 0) {
      throw new OperateRuntimeException(
          "Some ES shards failed. Ignoring search response and retrying, to prevent data loss.");
    }
  }

  private ImportBatch createImportBatch(final SearchResponse searchResponse) {
    final SearchHit[] hits = searchResponse.getHits().getHits();
    final List<HitEntity> newHits =
        Arrays.stream(hits)
            .map(
                h ->
                    new HitEntity().setIndex(h.getIndex()).setSourceAsString(h.getSourceAsString()))
            .toList();
    String indexName = null;
    if (hits.length > 0) {
      indexName = hits[hits.length - 1].getIndex();
    }
    return new ImportBatch(partitionId, importValueType, newHits, indexName);
  }

  private ImportBatch createImportBatch(final HitEntity[] hits) {
    String indexName = null;
    if (hits.length > 0) {
      indexName = hits[hits.length - 1].getIndex();
    }
    return new ImportBatch(partitionId, importValueType, Arrays.asList(hits), indexName);
  }

  private SearchRequest createSearchQuery(
      final String aliasName, final long positionFrom, final Long positionTo) {
    RangeQueryBuilder positionQ = rangeQuery(ImportPositionIndex.POSITION).gt(positionFrom);
    if (positionTo != null) {
      positionQ = positionQ.lte(positionTo);
    }
    final QueryBuilder queryBuilder =
        joinWithAnd(positionQ, termQuery(PARTITION_ID_FIELD_NAME, partitionId));

    SearchSourceBuilder searchSourceBuilder =
        new SearchSourceBuilder()
            .query(queryBuilder)
            .sort(ImportPositionIndex.POSITION, SortOrder.ASC);
    if (positionTo == null) {
      searchSourceBuilder = searchSourceBuilder.size(batchSizeThrottle.get());
    } else {
      LOGGER.debug(
          "Import batch reread was called. Data type {}, partitionId {}, positionFrom {}, positionTo {}.",
          importValueType,
          partitionId,
          positionFrom,
          positionTo);
      final int size = (int) (positionTo - positionFrom);
      searchSourceBuilder =
          searchSourceBuilder.size(
              size <= 0 || size > QUERY_MAX_SIZE
                  ? QUERY_MAX_SIZE
                  : size); // this size will be bigger than needed
    }
    return new SearchRequest(aliasName)
        .source(searchSourceBuilder)
        .routing(String.valueOf(partitionId))
        .requestCache(false);
  }

  private SearchResponse withTimer(final Callable<SearchResponse> callable) throws Exception {
    return metrics
        .getTimer(
            Metrics.TIMER_NAME_IMPORT_QUERY,
            Metrics.TAG_KEY_TYPE,
            importValueType.name(),
            Metrics.TAG_KEY_PARTITION,
            String.valueOf(partitionId))
        .recordCallable(callable);
  }

  private HitEntity[] withTimerSearchHits(final Callable<HitEntity[]> callable) throws Exception {
    return metrics
        .getTimer(
            Metrics.TIMER_NAME_IMPORT_QUERY,
            Metrics.TAG_KEY_TYPE,
            importValueType.name(),
            Metrics.TAG_KEY_PARTITION,
            String.valueOf(partitionId))
        .recordCallable(callable);
  }

  private boolean tryToScheduleImportJob(final ImportJob importJob, final boolean skipPendingJob) {
    return withReschedulingImportJobLock(
        () -> {
          var scheduled = false;
          var retries = 3;

          while (!scheduled && retries > 0) {
            scheduled = importJobs.offer(executeJob(importJob));
            retries = retries - 1;
          }

          pendingImportJob = skipPendingJob || scheduled ? null : importJob;
          if (scheduled && active == null) {
            executeNext();
          }

          return scheduled;
        });
  }

  private Callable<Boolean> executeJob(final ImportJob job) {
    return () -> {
      try {
        final var imported = job.call();
        if (imported) {
          executeNext();
          rescheduleRecordsReaderIfNecessary();
        } else {
          // retry the same job
          sleepFor(2000L);
          execute(active);
        }
        return imported;
      } catch (final Exception ex) {
        LOGGER.error("Exception occurred when importing data: " + ex.getMessage(), ex);
        // retry the same job
        sleepFor(2000L);
        execute(active);
        return false;
      }
    };
  }

  private void executeNext() {
    active = importJobs.poll();
    if (active != null) {
      importExecutor.submit(active);
      // TODO what to do with failing jobs
      LOGGER.debug("Submitted next job");
    }
  }

  private void execute(final Callable<Boolean> job) {
    importExecutor.submit(job);
    // TODO what to do with failing jobs
    LOGGER.debug("Submitted the same job");
  }

  private void rescheduleRecordsReaderIfNecessary() {
    withReschedulingImportJobLock(
        () -> {
          if (hasPendingImportJobToReschedule() && shouldReschedulePendingImportJob()) {
            startRescheduling();
            readersExecutor.submit(this::reschedulePendingImportJob);
          }
        });
  }

  private void reschedulePendingImportJob() {
    try {
      scheduleImportJob(pendingImportJob, false);
    } finally {
      // whatever happened (exception or not),
      // reset state and schedule reader so that
      // the reader starts from the last loaded
      // position again
      withReschedulingImportJobLock(
          () -> {
            pendingImportJob = null;
            completeRescheduling();
            readersExecutor.submit(this);
          });
    }
  }

  private boolean hasPendingImportJobToReschedule() {
    return pendingImportJob != null;
  }

  private boolean shouldReschedulePendingImportJob() {
    return !ongoingRescheduling;
  }

  private void startRescheduling() {
    ongoingRescheduling = true;
  }

  private void completeRescheduling() {
    ongoingRescheduling = false;
  }

  private void withReschedulingImportJobLock(final Runnable action) {
    withReschedulingImportJobLock(
        () -> {
          action.run();
          return null;
        });
  }

  private <T> T withReschedulingImportJobLock(final Callable<T> action) {
    try {
      schedulingImportJobLock.lock();
      return action.call();
    } catch (final Exception e) {
      throw new OperateRuntimeException(e);
    } finally {
      schedulingImportJobLock.unlock();
    }
  }
}
