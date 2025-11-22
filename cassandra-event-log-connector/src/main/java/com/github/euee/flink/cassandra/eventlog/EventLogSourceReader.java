package com.github.euee.flink.cassandra.eventlog;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Source reader for event log with two-phase behavior:
 *
 * <ol>
 *   <li><b>Bulk Load Phase</b>: Read all historical events for assigned shards
 *   <li><b>Polling Phase</b>: Continuously poll for new events
 * </ol>
 */
public class EventLogSourceReader
    implements org.apache.flink.api.connector.source.SourceReader<
        RowData, EventLogSourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(EventLogSourceReader.class);
  private static final DateTimeFormatter BUCKET_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");

  private final EventLogConnectionConfig config;
  private final String[] fieldNames;
  private final org.apache.flink.table.types.logical.LogicalType[] fieldTypes;
  private final SourceReaderContext context;

  private CqlSession session;
  private final Map<Integer, ShardReader> shardReaders = new HashMap<>();
  private final Queue<RowData> recordBuffer = new LinkedList<>();
  private long lastPollTime = 0;

  private boolean noMoreSplits = false;

  public EventLogSourceReader(
      EventLogConnectionConfig config,
      String[] fieldNames,
      org.apache.flink.table.types.logical.LogicalType[] fieldTypes,
      SourceReaderContext context) {
    this.config = config;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
    this.context = context;
  }

  @Override
  public void start() {
    try {
      LOG.info(
          "Connecting to Cassandra at {}:{} for keyspace {}",
          config.getHost(),
          config.getPort(),
          config.getKeyspace());

      session =
          CqlSession.builder()
              .addContactPoint(new InetSocketAddress(config.getHost(), config.getPort()))
              .withLocalDatacenter(config.getDatacenter())
              .withAuthCredentials(config.getUsername(), config.getPassword())
              .withKeyspace(config.getKeyspace())
              .build();

      LOG.info("Successfully connected to Cassandra");
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect to Cassandra", e);
    }
  }

  @Override
  public InputStatus pollNext(org.apache.flink.api.connector.source.ReaderOutput<RowData> output)
      throws Exception {

    // If we have buffered records, emit them first
    if (!recordBuffer.isEmpty()) {
      RowData record = recordBuffer.poll();
      output.collect(record);
      return InputStatus.MORE_AVAILABLE;
    }

    // Check if it's time to poll (respecting poll interval)
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastPollTime < config.getPollIntervalMs()) {
      return InputStatus.NOTHING_AVAILABLE;
    }

    // Poll all shard readers
    boolean hasData = false;
    for (ShardReader reader : shardReaders.values()) {
      List<RowData> newRecords = reader.pollEvents();
      if (!newRecords.isEmpty()) {
        recordBuffer.addAll(newRecords);
        hasData = true;
      }
    }

    lastPollTime = currentTime;

    if (hasData && !recordBuffer.isEmpty()) {
      RowData record = recordBuffer.poll();
      output.collect(record);
      return InputStatus.MORE_AVAILABLE;
    }

    // For unbounded source, always return NOTHING_AVAILABLE when no data
    // (never END_OF_INPUT)
    return InputStatus.NOTHING_AVAILABLE;
  }

  @Override
  public List<EventLogSourceSplit> snapshotState(long checkpointId) {
    LOG.debug("Snapshotting state for checkpoint {}", checkpointId);
    List<EventLogSourceSplit> splits = new ArrayList<>();
    for (ShardReader reader : shardReaders.values()) {
      splits.add(reader.getCurrentSplit());
    }
    return splits;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    // Return a completed future - data is always potentially available
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void addSplits(List<EventLogSourceSplit> splits) {
    LOG.info("Adding {} splits to reader", splits.size());
    for (EventLogSourceSplit split : splits) {
      ShardReader reader = new ShardReader(split);
      shardReaders.put(split.getShardId(), reader);
      LOG.info("Added shard reader for shard {}", split.getShardId());
    }
  }

  @Override
  public void notifyNoMoreSplits() {
    LOG.info("No more splits will be assigned");
    noMoreSplits = true;
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      session.close();
      LOG.info("Closed Cassandra session");
    }
  }

  /** Reader for a single shard that handles bulk load + polling. */
  private class ShardReader {
    private final int shardId;
    private String lastEventTime; // TimeUUID as string
    private boolean bulkLoadComplete = false;

    ShardReader(EventLogSourceSplit split) {
      this.shardId = split.getShardId();
      this.lastEventTime = split.getLastEventTime();
      this.bulkLoadComplete = (lastEventTime != null); // If restored, bulk load already done
    }

    EventLogSourceSplit getCurrentSplit() {
      return new EventLogSourceSplit(shardId, lastEventTime);
    }

    /**
     * Poll for new events. Returns list of new events as RowData.
     *
     * <p>Phase 1 (bulk load): Query all historical events Phase 2 (polling): Query only new events
     * since lastEventTime
     */
    List<RowData> pollEvents() {
      List<RowData> results = new ArrayList<>();

      if (!bulkLoadComplete) {
        // Phase 1: Bulk load all historical events
        LOG.info("Starting bulk load for shard {}", shardId);
        results.addAll(getAllEvents());
        bulkLoadComplete = true;
        LOG.info("Bulk load complete for shard {}, last event time: {}", shardId, lastEventTime);
      } else {
        // Phase 2: Poll for new events
        results.addAll(getTailEvents());
      }

      return results;
    }

    /**
     * Get all historical events for this shard (equivalent to Python's get_all_events).
     *
     * <p>Queries all bucket hours from epoch to now.
     */
    private List<RowData> getAllEvents() {
      List<RowData> allEvents = new ArrayList<>();

      // Generate bucket hours from a reasonable start time to now
      // For simplicity, start from 24 hours ago (adjust as needed)
      ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
      ZonedDateTime startTime = now.minusDays(30); // Look back 30 days for historical data

      List<String> buckets = generateBuckets(startTime, now);

      LOG.debug("Querying {} buckets for shard {} bulk load", buckets.size(), shardId);

      for (String bucket : buckets) {
        List<RowData> bucketEvents = queryBucket(bucket, null); // null = get all events
        allEvents.addAll(bucketEvents);
      }

      return allEvents;
    }

    /**
     * Get new events since lastEventTime (equivalent to Python's tail_events).
     *
     * <p>Queries current hour and lookback hours for new events.
     */
    private List<RowData> getTailEvents() {
      List<RowData> newEvents = new ArrayList<>();

      ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
      ZonedDateTime startTime = now.minusHours(config.getLookbackHours());

      List<String> buckets = generateBuckets(startTime, now);

      for (String bucket : buckets) {
        List<RowData> bucketEvents = queryBucket(bucket, lastEventTime);
        newEvents.addAll(bucketEvents);
      }

      return newEvents;
    }

    /**
     * Query a specific bucket for events.
     *
     * @param bucketHour The bucket hour (yyyy-MM-dd-HH format)
     * @param afterEventTime If not null, only get events after this timeuuid
     */
    private List<RowData> queryBucket(String bucketHour, String afterEventTime) {
      List<RowData> events = new ArrayList<>();

      try {
        // Build query: SELECT * FROM table WHERE bucket_hour = ? AND shard = ? AND event_time > ?
        String query =
            String.format(
                "SELECT * FROM %s.%s WHERE bucket_hour = ? AND shard = ?",
                config.getKeyspace(), config.getTable());

        if (afterEventTime != null) {
          query += " AND event_time > ?";
        }

        PreparedStatement prepared = session.prepare(query);
        BoundStatement bound = prepared.bind(bucketHour, shardId);

        if (afterEventTime != null) {
          // Parse timeuuid string back to UUID
          UUID afterUuid = UUID.fromString(afterEventTime);
          bound = prepared.bind(bucketHour, shardId, afterUuid);
        }

        bound = bound.setPageSize(config.getFetchSize());

        ResultSet resultSet = session.execute(bound);

        for (Row row : resultSet) {
          // Convert row to RowData
          RowData rowData = EventLogTypeConverter.convertRow(row, fieldNames, fieldTypes);
          events.add(rowData);

          // Update lastEventTime to the latest event_time (timeuuid)
          // Assuming event_time is one of the fields
          UUID eventTimeUuid = row.getUuid("event_time");
          if (eventTimeUuid != null) {
            lastEventTime = eventTimeUuid.toString();
          }
        }

        if (!events.isEmpty()) {
          LOG.debug(
              "Retrieved {} events from bucket {} shard {}, last event time: {}",
              events.size(),
              bucketHour,
              shardId,
              lastEventTime);
        }

      } catch (Exception e) {
        LOG.error("Failed to query bucket {} shard {}", bucketHour, shardId, e);
      }

      return events;
    }

    /** Generate list of bucket hours between start and end time. */
    private List<String> generateBuckets(ZonedDateTime start, ZonedDateTime end) {
      List<String> buckets = new ArrayList<>();
      ZonedDateTime current = start.withMinute(0).withSecond(0).withNano(0);

      while (!current.isAfter(end)) {
        buckets.add(current.format(BUCKET_FORMATTER));
        current = current.plusHours(1);
      }

      return buckets;
    }
  }
}
