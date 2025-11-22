# Flink Cassandra Event Log Connector

A production-ready Apache Flink **unbounded streaming** connector for Cassandra event log tables with continuous polling support.

**Built for Apache Flink 2.1.1** | **Requires Java 11+**

## Features

- ✅ **Two-Phase Reading Strategy**:
  - Phase 1: Bulk load all historical events
  - Phase 2: Continuous polling for new events
- ✅ **Unbounded Streaming Source** (not batch)
- ✅ **Automatic Event Deduplication** via TimeUUID tracking
- ✅ **Shard-Based Parallelism** (configurable number of shards)
- ✅ **Time-Bucketed Queries** (bucket_hour based partitioning)
- ✅ **Fault-Tolerant** with checkpointing support
- ✅ **Configurable Polling Interval** and lookback window
- ✅ **TimeUUID Support** for event ordering and timestamp extraction

## Event Log Table Schema

This connector is designed for Cassandra event log tables with the following schema pattern:

```sql
CREATE TABLE events.event_log (
    bucket_hour text,        -- Format: "yyyy-MM-dd-HH" (e.g., "2025-11-22-14")
    shard int,               -- Shard ID for parallelism (0-15 by default)
    event_time timeuuid,     -- TimeUUID for ordering and timestamp
    event_id text,           -- Optional event identifier
    event_type text,         -- Event type/category
    payload text,            -- JSON or other payload
    PRIMARY KEY ((bucket_hour, shard), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
```

**Key Design Elements:**

- **bucket_hour**: Partitioning key for time-based data distribution
- **shard**: Enables parallel processing across multiple readers
- **event_time**: TimeUUID provides both ordering and timestamp information
- **Clustering**: Events ordered by time within each bucket+shard partition

## Quick Start

### 1. Build the Connector

```bash
mvn clean package
```

### 2. Add to Flink

Copy the JAR to your Flink's `lib/` directory:

```bash
cp target/flink-cassandra-event-log-connector-0.1.0.jar $FLINK_HOME/lib/
```

### 3. Use in Flink SQL

```sql
CREATE TABLE event_stream (
  bucket_hour STRING,
  shard INT,
  event_time STRING,      -- TimeUUID as string
  event_id STRING,
  event_type STRING,
  payload STRING
) WITH (
  'connector' = 'cassandra-event-log-connector',
  'keyspace' = 'events',
  'table' = 'event_log',
  'cassandra_host' = 'localhost',
  'cassandra_port' = '9042',
  'cassandra_user' = 'cassandra',
  'cassandra_password' = 'cassandra',
  'num-shards' = '16',
  'poll-interval-ms' = '1000',
  'lookback-hours' = '1'
);

-- Continuously stream events (unbounded query)
SELECT
  event_id,
  event_type,
  payload,
  event_time
FROM event_stream;
```

## Configuration Options

### Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'cassandra-event-log-connector'` |
| `keyspace` | String | Cassandra keyspace name |
| `table` | String | Cassandra event log table name |
| `cassandra_host` | String | Cassandra contact point host |
| `cassandra_user` | String | Username for authentication |
| `cassandra_password` | String | Password for authentication |

### Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cassandra_port` | Integer | `9042` | Cassandra contact point port |
| `datacenter` | String | `'datacenter1'` | Cassandra datacenter name |
| `num-shards` | Integer | `16` | Number of shards in event log (0 to N-1) |
| `poll-interval-ms` | Long | `1000` | Polling interval in milliseconds |
| `fetch-size` | Integer | `5000` | Rows per Cassandra query |
| `lookback-hours` | Integer | `1` | Hours to look back when polling for new events |

## How It Works

### Two-Phase Reading Strategy

#### Phase 1: Bulk Load (Initial Historical Read)

When the connector starts, each shard reader:

1. Queries all bucket hours from the past (e.g., last 30 days)
2. Reads ALL events from assigned shards
3. Tracks the last `event_time` (TimeUUID) per shard
4. Transitions to polling mode

```java
// Pseudo-code for bulk load
for (String bucket : generateHistoricalBuckets()) {
  SELECT * FROM event_log
  WHERE bucket_hour = ? AND shard = ?
  ORDER BY event_time
}
```

#### Phase 2: Continuous Polling

After bulk load, each shard reader continuously:

1. Generates current + lookback bucket hours
2. Queries only events newer than last `event_time`
3. Sleeps for `poll-interval-ms`
4. Repeats indefinitely (unbounded)

```java
// Pseudo-code for polling
while (true) {
  for (String bucket : getCurrentAndLookbackBuckets()) {
    SELECT * FROM event_log
    WHERE bucket_hour = ? AND shard = ? AND event_time > ?
    ORDER BY event_time
  }
  sleep(pollIntervalMs);
}
```

### Shard-Based Parallelism

The connector distributes shards across Flink parallel instances:

- **16 shards, parallelism 4**: Each reader handles 4 shards
- **16 shards, parallelism 8**: Each reader handles 2 shards
- **16 shards, parallelism 16**: Each reader handles 1 shard

Example shard distribution:

```
Reader 0: shards [0, 1, 2, 3]
Reader 1: shards [4, 5, 6, 7]
Reader 2: shards [8, 9, 10, 11]
Reader 3: shards [12, 13, 14, 15]
```

### TimeUUID Handling

TimeUUID (UUID version 1) provides:

1. **Timestamp**: Extracted and converted to Flink TIMESTAMP
2. **Ordering**: Natural chronological ordering
3. **Uniqueness**: No event duplication

The connector automatically:
- Converts TimeUUID to STRING for easy handling
- Extracts timestamp when mapped to TIMESTAMP fields
- Uses TimeUUID for incremental polling (`event_time > last_uuid`)

## Architecture

### Component Overview

```
EventLogTableSourceFactory
  └─> EventLogTableSource
       └─> EventLogSource (Unbounded)
            ├─> EventLogSplitEnumerator (assigns shards)
            └─> EventLogSourceReader (polls events)
                 ├─> ShardReader (shard 0)
                 ├─> ShardReader (shard 1)
                 └─> ShardReader (shard N)
```

### Key Classes

- **EventLogSource**: Main unbounded source implementing Flink 2.x Source API
- **EventLogSourceReader**: Manages multiple shard readers with polling logic
- **ShardReader**: Handles bulk load + polling for a single shard
- **EventLogSplitEnumerator**: Distributes shards across parallel readers
- **EventLogSourceSplit**: Represents a shard with state (last event time)
- **EventLogTypeConverter**: Converts Cassandra types to Flink types

## Performance Tuning

### Parallelism

Control the number of parallel readers (must be ≤ num-shards):

```sql
-- Option 1: In table DDL
CREATE TABLE event_stream (...) WITH (
  'num-shards' = '16',
  ...
);

-- Option 2: At query level
SELECT * FROM event_stream /*+ OPTIONS('parallelism'='8') */;
```

**Best Practice**: Set parallelism = num-shards for maximum throughput.

### Poll Interval

Balance between latency and Cassandra load:

```sql
'poll-interval-ms' = '500'   -- Low latency (500ms), higher load
'poll-interval-ms' = '5000'  -- Higher latency (5s), lower load
```

### Fetch Size

Control page size for Cassandra queries:

```sql
'fetch-size' = '10000'  -- Larger pages, fewer round trips, more memory
'fetch-size' = '1000'   -- Smaller pages, more round trips, less memory
```

### Lookback Hours

Control how far back to query when polling:

```sql
'lookback-hours' = '1'  -- Query current + previous 1 hour (default)
'lookback-hours' = '2'  -- Query current + previous 2 hours (catch late events)
```

## Example: Complete Usage

### Create Event Log Table in Cassandra

```sql
CREATE KEYSPACE IF NOT EXISTS events
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE events.event_log (
    bucket_hour text,
    shard int,
    event_time timeuuid,
    event_id text,
    event_type text,
    user_id text,
    payload text,
    PRIMARY KEY ((bucket_hour, shard), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
```

### Create Flink Table and Query

```sql
-- Create unbounded streaming table
CREATE TABLE event_stream (
  bucket_hour STRING,
  shard INT,
  event_time STRING,
  event_id STRING,
  event_type STRING,
  user_id STRING,
  payload STRING
) WITH (
  'connector' = 'cassandra-event-log-connector',
  'keyspace' = 'events',
  'table' = 'event_log',
  'cassandra_host' = '10.0.0.100',
  'cassandra_port' = '9042',
  'cassandra_user' = 'flink_reader',
  'cassandra_password' = 'secure_password',
  'datacenter' = 'dc1',
  'num-shards' = '16',
  'poll-interval-ms' = '1000',
  'fetch-size' = '5000',
  'lookback-hours' = '1'
);

-- Continuously stream all events
SELECT * FROM event_stream;

-- Filter by event type (streaming)
SELECT
  event_id,
  user_id,
  payload
FROM event_stream
WHERE event_type = 'user_login';

-- Aggregate events in tumbling windows
SELECT
  event_type,
  COUNT(*) as event_count,
  TUMBLE_START(TO_TIMESTAMP(event_time), INTERVAL '1' MINUTE) as window_start
FROM event_stream
GROUP BY
  event_type,
  TUMBLE(TO_TIMESTAMP(event_time), INTERVAL '1' MINUTE);
```

### Join with Dimension Table

```sql
-- Create dimension table (bounded)
CREATE TABLE users (
  user_id STRING,
  username STRING,
  email STRING
) WITH (
  'connector' = 'cassandra-table-connector',  -- Use bounded connector
  'keyspace' = 'users',
  'table' = 'user_profiles',
  ...
);

-- Join streaming events with user dimension
SELECT
  e.event_id,
  e.event_type,
  u.username,
  u.email,
  e.payload
FROM event_stream e
LEFT JOIN users FOR SYSTEM_TIME AS OF e.event_time AS u
  ON e.user_id = u.user_id;
```

## Checkpointing and Fault Tolerance

The connector is fully fault-tolerant with Flink's checkpointing:

```java
// Enable checkpointing in Flink
env.enableCheckpointing(60000); // Checkpoint every 60 seconds

// On recovery:
// - Enumerator restores pending splits
// - Each reader restores last_event_time per shard
// - Polling resumes from last checkpoint
```

**State Stored in Checkpoints:**

- Enumerator: List of pending splits (shards not yet assigned)
- Reader: Last `event_time` (TimeUUID) per shard

## Limitations

1. **Read-Only**: This connector only supports reading (no writes to Cassandra)
2. **TimeUUID Required**: Event log table must use TimeUUID for `event_time`
3. **Bucket Format**: Assumes `bucket_hour` in "yyyy-MM-dd-HH" format
4. **Append-Only**: Assumes events are never updated or deleted
5. **No Predicate Pushdown**: Filtering happens in Flink, not Cassandra

## Troubleshooting

### No Events Appearing

**Problem**: Connector starts but no events are emitted

**Solutions**:
1. Check `num-shards` matches your Cassandra table schema
2. Verify `bucket_hour` format is "yyyy-MM-dd-HH"
3. Ensure events exist in recent buckets (check `lookback-hours`)
4. Increase logging: `rootLogger.level = DEBUG`

### High Cassandra Load

**Problem**: Too many queries hitting Cassandra

**Solutions**:
1. Increase `poll-interval-ms` (reduce polling frequency)
2. Reduce `lookback-hours` (query fewer buckets)
3. Reduce parallelism (fewer readers = fewer queries)

### Late Events Missing

**Problem**: Some events don't appear in stream

**Solutions**:
1. Increase `lookback-hours` to catch late-arriving events
2. Check for clock skew between event producers and Flink

## Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd cassandra-event-log-connector

# Build
mvn clean package

# Run tests (requires Cassandra running)
mvn test

# Skip tests
mvn clean package -DskipTests
```

## Dependencies

- Apache Flink 2.1.1
- DataStax Java Driver 4.17.0
- Java 11 or later (Java 17 recommended)

## Comparison with cassandra-table-connector

| Feature | event-log-connector | table-connector |
|---------|-------------------|-----------------|
| Boundedness | **Unbounded** (streaming) | Bounded (batch) |
| Use Case | Continuous event ingestion | One-time table read |
| Polling | ✅ Yes | ❌ No |
| TimeUUID | ✅ Required | Optional |
| Shard-Based | ✅ Yes | ❌ No |
| Checkpointing | Per-shard state | Split ranges |

**When to Use Which:**

- **event-log-connector**: Real-time event streams, CDC, logs, metrics
- **table-connector**: Dimension tables, reference data, batch ETL

## License

Apache License 2.0

## Contributing

Contributions welcome! Please open issues or pull requests.
