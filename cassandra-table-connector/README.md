# Flink Cassandra Table Connector

A production-ready Apache Flink table connector for reading data from Apache Cassandra tables.

**Built for Apache Flink 2.1.1** | **Requires Java 11+**

## Features

- ✅ Reads from Cassandra tables using Flink 2.x Source API
- ✅ Supports parallel reading with configurable parallelism
- ✅ Automatic type conversion from Cassandra to Flink types
- ✅ Fault-tolerant with checkpointing support
- ✅ Configurable fetch size for optimized performance
- ✅ Secure authentication with username/password

## Quick Start

### 1. Build the Connector

```bash
mvn clean package
```

### 2. Add to Flink

Copy the JAR to your Flink's `lib/` directory:

```bash
cp target/flink-cassandra-table-connector-0.1.0.jar $FLINK_HOME/lib/
```

### 3. Use in Flink SQL

```sql
CREATE TABLE flink_table_backed_by_cassandra (
  user_id STRING,
  feedback_id STRING,
  feedback STRING,
  PRIMARY KEY (feedback_id) NOT ENFORCED
) WITH (
  'connector' = 'cassandra-table-connector',
  'keyspace' = 'users',
  'table' = 'end_user_feedback',
  'cassandra_host' = 'localhost',
  'cassandra_port' = '9042',
  'cassandra_user' = 'cassandra',
  'cassandra_password' = 'cassandra'
);

SELECT * FROM flink_table_backed_by_cassandra;
```

## Configuration Options

### Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'cassandra-table-connector'` |
| `keyspace` | String | Cassandra keyspace name |
| `table` | String | Cassandra table name |
| `cassandra_host` | String | Cassandra contact point host |
| `cassandra_user` | String | Username for authentication |
| `cassandra_password` | String | Password for authentication |

### Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cassandra_port` | Integer | `9042` | Cassandra contact point port |
| `datacenter` | String | `'datacenter1'` | Cassandra datacenter name |
| `fetch-size` | Integer | `5000` | Number of rows to fetch per request |
| `parallelism` | Integer | `1` | Number of parallel readers |

## Column Name Mapping

The connector expects Flink column names to match Cassandra column names. For example:

**Cassandra Table:**
```sql
CREATE TABLE users.end_user_feedback (
    userid text,
    feedbackid uuid,
    feedback text,
    PRIMARY KEY(userid, feedbackid)
);
```

**Flink Table (matching names):**
```sql
CREATE TABLE feedback (
  userid STRING,
  feedbackid STRING,
  feedback STRING
) WITH (...)
```

**Flink Table (with different names - requires mapping):**
```sql
CREATE TABLE feedback (
  user_id STRING,     -- Maps to: userid
  feedback_id STRING,  -- Maps to: feedbackid
  feedback STRING      -- Maps to: feedback
) WITH (...)
```

*Note: Column name mapping can be customized in `CassandraTypeConverter.mapColumnName()`*

## Supported Data Types

| Cassandra Type | Flink Type |
|----------------|------------|
| `text`, `varchar` | `STRING` |
| `uuid`, `timeuuid` | `STRING` |
| `boolean` | `BOOLEAN` |
| `tinyint` | `TINYINT` |
| `smallint` | `SMALLINT` |
| `int` | `INT` |
| `bigint` | `BIGINT` |
| `float` | `FLOAT` |
| `double` | `DOUBLE` |
| `decimal` | `DECIMAL` |
| `date` | `DATE` |
| `time` | `TIME` |
| `timestamp` | `TIMESTAMP` |

## Architecture

The connector uses Flink 2.x Source API with:

- **CassandraSource**: Main source interface
- **CassandraSourceReader**: Reads data from Cassandra using DataStax Java Driver
- **CassandraSplitEnumerator**: Manages parallel reading with token-based splits
- **CassandraTypeConverter**: Converts Cassandra types to Flink internal types

## Performance Tuning

### Parallelism

Control the number of parallel readers:

```sql
'parallelism' = '4'  -- Read with 4 parallel tasks
```

Higher parallelism = faster reads but more connections to Cassandra.

### Fetch Size

Control how many rows are fetched per request:

```sql
'fetch-size' = '10000'  -- Fetch 10k rows per page
```

Larger fetch size = fewer round trips but more memory usage.

## Example: Complete Usage

```sql
-- Create Flink table backed by Cassandra
CREATE TABLE user_feedback (
  user_id STRING,
  feedback_id STRING,
  feedback STRING,
  timestamp TIMESTAMP(3)
) WITH (
  'connector' = 'cassandra-table-connector',
  'keyspace' = 'users',
  'table' = 'end_user_feedback',
  'cassandra_host' = '10.0.0.100',
  'cassandra_port' = '9042',
  'cassandra_user' = 'flink_reader',
  'cassandra_password' = 'secure_password',
  'datacenter' = 'dc1',
  'fetch-size' = '5000',
  'parallelism' = '4'
);

-- Query recent feedback
SELECT
  user_id,
  feedback,
  timestamp
FROM user_feedback
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1' DAY
LIMIT 100;

-- Aggregate feedback by user
SELECT
  user_id,
  COUNT(*) as feedback_count
FROM user_feedback
GROUP BY user_id;

-- Join with another table
CREATE TABLE users (
  user_id STRING,
  username STRING,
  email STRING
) WITH (...);

SELECT
  u.username,
  u.email,
  f.feedback,
  f.timestamp
FROM user_feedback f
JOIN users u ON f.user_id = u.user_id;
```

## Limitations

1. **Read-only**: This connector only supports reading from Cassandra (no writes)
2. **Token Range Splits**: Currently creates simple token range splits; production use may benefit from querying actual Cassandra token ranges
3. **No Predicate Pushdown**: Filtering happens in Flink, not pushed to Cassandra (can be added)
4. **Primary Key Required**: Cassandra table must have a primary key defined

## Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd cassandra-table-connector

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
- Java 11 or later

## Troubleshooting

### Connection Issues

**Error**: `Could not connect to Cassandra`

**Solution**:
- Verify `cassandra_host` and `cassandra_port`
- Check network connectivity
- Ensure Cassandra is running

### Authentication Failures

**Error**: `Authentication error`

**Solution**:
- Verify `cassandra_user` and `cassandra_password`
- Check user permissions in Cassandra

### Type Conversion Errors

**Error**: `Failed to convert column X to Flink type Y`

**Solution**:
- Check that Flink type matches Cassandra type
- Refer to supported data types table above
- Consider using STRING type for complex Cassandra types

## License

Apache License 2.0

## Contributing

Contributions welcome! Please open issues or pull requests.
