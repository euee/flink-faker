package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.net.InetSocketAddress;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end tests for EventLog connector with embedded Cassandra.
 *
 * <p>Note: These tests require significant resources and time. They are disabled by default. To
 * run them, remove the @Disabled annotation.
 */
@Disabled("E2E tests are resource-intensive and require embedded Cassandra")
class EventLogConnectorE2ETest {

  private static final DateTimeFormatter BUCKET_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
  private static final String KEYSPACE = "test_events";
  private static final String TABLE = "event_log";
  private static final int CASSANDRA_PORT = 9142; // Default CassandraUnit port

  private static CqlSession session;

  @BeforeAll
  static void setUpCassandra() throws Exception {
    // Start embedded Cassandra using CassandraUnit
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();

    // Connect to Cassandra
    session =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress("127.0.0.1", CASSANDRA_PORT))
            .withLocalDatacenter("datacenter1")
            .build();

    // Create keyspace
    session.execute(
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            KEYSPACE));

    // Create event log table
    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s ("
                + "bucket_hour text, "
                + "shard int, "
                + "event_time timeuuid, "
                + "event_id text, "
                + "event_type text, "
                + "payload text, "
                + "PRIMARY KEY ((bucket_hour, shard), event_time)) "
                + "WITH CLUSTERING ORDER BY (event_time DESC)",
            KEYSPACE, TABLE));

    // Insert test data
    insertTestEvents();
  }

  @AfterAll
  static void tearDownCassandra() {
    if (session != null) {
      session.close();
    }
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  private static void insertTestEvents() {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    String currentBucket = now.format(BUCKET_FORMATTER);

    // Insert 100 events across 4 shards
    for (int i = 0; i < 100; i++) {
      int shard = i % 4; // Use 4 shards
      UUID eventTime = Uuids.timeBased();

      String insertQuery =
          String.format(
              "INSERT INTO %s.%s (bucket_hour, shard, event_time, event_id, event_type, payload) "
                  + "VALUES (?, ?, ?, ?, ?, ?)",
              KEYSPACE, TABLE);

      session.execute(
          SimpleStatement.newInstance(
              insertQuery,
              currentBucket,
              shard,
              eventTime,
              "event-" + i,
              "test_event",
              "{\"index\":" + i + "}"));
    }

    System.out.println("Inserted 100 test events into Cassandra");
  }

  @Test
  @Timeout(60) // 60 seconds timeout
  void testReadEventsFromCassandra() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2); // Use 2 parallel readers

    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

    // Create Flink table backed by Cassandra event log
    String createTableDDL =
        String.format(
            "CREATE TABLE event_stream ("
                + "  bucket_hour STRING,"
                + "  shard INT,"
                + "  event_time STRING,"
                + "  event_id STRING,"
                + "  event_type STRING,"
                + "  payload STRING"
                + ") WITH ("
                + "  'connector' = 'cassandra-event-log-connector',"
                + "  'cassandra_host' = '127.0.0.1',"
                + "  'cassandra_port' = '%d',"
                + "  'cassandra_user' = 'cassandra',"
                + "  'cassandra_password' = 'cassandra',"
                + "  'keyspace' = '%s',"
                + "  'table' = '%s',"
                + "  'num-shards' = '4',"
                + "  'poll-interval-ms' = '1000',"
                + "  'lookback-hours' = '1'"
                + ")",
            CASSANDRA_PORT, KEYSPACE, TABLE);

    tableEnv.executeSql(createTableDDL);

    // Query the table - read first 50 events
    Table result = tableEnv.sqlQuery("SELECT event_id, event_type, payload FROM event_stream");

    // Collect results (this will start the Flink job)
    CloseableIterator<Row> iterator = result.execute().collect();

    List<Row> rows = new ArrayList<>();
    int count = 0;
    while (iterator.hasNext() && count < 50) {
      rows.add(iterator.next());
      count++;
    }

    iterator.close();

    // Verify we got events
    assertThat(rows).isNotEmpty();
    assertThat(rows.size()).isGreaterThan(0);

    // Verify event structure
    for (Row row : rows) {
      assertThat(row.getArity()).isEqualTo(3);
      assertThat(row.getField(0)).isNotNull(); // event_id
      assertThat(row.getField(1)).isEqualTo("test_event"); // event_type
      assertThat(row.getField(2)).isNotNull(); // payload
    }

    System.out.println("Successfully read " + rows.size() + " events from Cassandra");
  }

  @Test
  @Timeout(60)
  void testFilterEvents() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

    String createTableDDL =
        String.format(
            "CREATE TABLE event_stream ("
                + "  bucket_hour STRING,"
                + "  shard INT,"
                + "  event_time STRING,"
                + "  event_id STRING,"
                + "  event_type STRING,"
                + "  payload STRING"
                + ") WITH ("
                + "  'connector' = 'cassandra-event-log-connector',"
                + "  'cassandra_host' = '127.0.0.1',"
                + "  'cassandra_port' = '%d',"
                + "  'cassandra_user' = 'cassandra',"
                + "  'cassandra_password' = 'cassandra',"
                + "  'keyspace' = '%s',"
                + "  'table' = '%s',"
                + "  'num-shards' = '4',"
                + "  'poll-interval-ms' = '500'"
                + ")",
            CASSANDRA_PORT, KEYSPACE, TABLE);

    tableEnv.executeSql(createTableDDL);

    // Filter by event_type
    Table result =
        tableEnv.sqlQuery(
            "SELECT event_id FROM event_stream WHERE event_type = 'test_event' LIMIT 10");

    CloseableIterator<Row> iterator = result.execute().collect();

    List<Row> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      rows.add(iterator.next());
    }

    iterator.close();

    assertThat(rows).hasSize(10);

    System.out.println("Successfully filtered and read " + rows.size() + " events");
  }

  @Test
  @Timeout(60)
  void testReadFromSpecificShard() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

    String createTableDDL =
        String.format(
            "CREATE TABLE event_stream ("
                + "  bucket_hour STRING,"
                + "  shard INT,"
                + "  event_time STRING,"
                + "  event_id STRING,"
                + "  event_type STRING,"
                + "  payload STRING"
                + ") WITH ("
                + "  'connector' = 'cassandra-event-log-connector',"
                + "  'cassandra_host' = '127.0.0.1',"
                + "  'cassandra_port' = '%d',"
                + "  'cassandra_user' = 'cassandra',"
                + "  'cassandra_password' = 'cassandra',"
                + "  'keyspace' = '%s',"
                + "  'table' = '%s',"
                + "  'num-shards' = '4'"
                + ")",
            CASSANDRA_PORT, KEYSPACE, TABLE);

    tableEnv.executeSql(createTableDDL);

    // Read from shard 0 only
    Table result =
        tableEnv.sqlQuery("SELECT event_id, shard FROM event_stream WHERE shard = 0 LIMIT 20");

    CloseableIterator<Row> iterator = result.execute().collect();

    List<Row> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      rows.add(iterator.next());
    }

    iterator.close();

    assertThat(rows).isNotEmpty();

    // Verify all rows are from shard 0
    for (Row row : rows) {
      assertThat(row.getField(1)).isEqualTo(0);
    }

    System.out.println("Successfully read " + rows.size() + " events from shard 0");
  }
}
