package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

/** Integration tests for {@link EventLogTableSourceFactory}. */
class EventLogTableSourceFactoryTest {

  @Test
  void testFactoryIdentifier() {
    EventLogTableSourceFactory factory = new EventLogTableSourceFactory();
    assertThat(factory.factoryIdentifier()).isEqualTo("cassandra-event-log-connector");
  }

  @Test
  void testRequiredOptions() {
    EventLogTableSourceFactory factory = new EventLogTableSourceFactory();

    assertThat(factory.requiredOptions())
        .containsExactlyInAnyOrder(
            EventLogTableSourceFactory.CASSANDRA_HOST,
            EventLogTableSourceFactory.CASSANDRA_USER,
            EventLogTableSourceFactory.CASSANDRA_PASSWORD,
            EventLogTableSourceFactory.KEYSPACE,
            EventLogTableSourceFactory.TABLE);
  }

  @Test
  void testOptionalOptions() {
    EventLogTableSourceFactory factory = new EventLogTableSourceFactory();

    assertThat(factory.optionalOptions())
        .containsExactlyInAnyOrder(
            EventLogTableSourceFactory.CASSANDRA_PORT,
            EventLogTableSourceFactory.DATACENTER,
            EventLogTableSourceFactory.NUM_SHARDS,
            EventLogTableSourceFactory.POLL_INTERVAL_MS,
            EventLogTableSourceFactory.FETCH_SIZE,
            EventLogTableSourceFactory.LOOKBACK_HOURS);
  }

  @Test
  void testCreateTableSourceWithAllOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", "cassandra-event-log-connector");
    options.put("cassandra_host", "localhost");
    options.put("cassandra_port", "9042");
    options.put("cassandra_user", "cassandra");
    options.put("cassandra_password", "cassandra");
    options.put("keyspace", "events");
    options.put("table", "event_log");
    options.put("datacenter", "dc1");
    options.put("num-shards", "32");
    options.put("poll-interval-ms", "2000");
    options.put("fetch-size", "10000");
    options.put("lookback-hours", "2");

    ResolvedSchema schema =
        ResolvedSchema.of(
            Column.physical("bucket_hour", DataTypes.STRING()),
            Column.physical("shard", DataTypes.INT()),
            Column.physical("event_time", DataTypes.STRING()),
            Column.physical("payload", DataTypes.STRING()));

    EventLogTableSourceFactory factory = new EventLogTableSourceFactory();
    DynamicTableSource.Context context = createContext(options, schema);

    DynamicTableSource source = factory.createDynamicTableSource(context);

    assertThat(source).isInstanceOf(EventLogTableSource.class);

    EventLogTableSource tableSource = (EventLogTableSource) source;
    String summary = tableSource.asSummaryString();

    assertThat(summary).contains("keyspace=events");
    assertThat(summary).contains("table=event_log");
    assertThat(summary).contains("numShards=32");
    assertThat(summary).contains("pollIntervalMs=2000");
  }

  @Test
  void testCreateTableSourceWithDefaultOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", "cassandra-event-log-connector");
    options.put("cassandra_host", "cassandra-host");
    options.put("cassandra_user", "user");
    options.put("cassandra_password", "password");
    options.put("keyspace", "ks1");
    options.put("table", "table1");

    ResolvedSchema schema =
        ResolvedSchema.of(
            Column.physical("bucket_hour", DataTypes.STRING()),
            Column.physical("shard", DataTypes.INT()));

    EventLogTableSourceFactory factory = new EventLogTableSourceFactory();
    DynamicTableSource.Context context = createContext(options, schema);

    DynamicTableSource source = factory.createDynamicTableSource(context);

    assertThat(source).isInstanceOf(EventLogTableSource.class);

    // Verify defaults are applied (shown in summary)
    EventLogTableSource tableSource = (EventLogTableSource) source;
    String summary = tableSource.asSummaryString();

    assertThat(summary).contains("numShards=16"); // default
    assertThat(summary).contains("pollIntervalMs=1000"); // default
  }

  @Test
  void testCreateTableSourceMissingRequiredOption() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", "cassandra-event-log-connector");
    options.put("cassandra_host", "localhost");
    // Missing cassandra_user, cassandra_password, keyspace, table

    ResolvedSchema schema =
        ResolvedSchema.of(Column.physical("bucket_hour", DataTypes.STRING()));

    EventLogTableSourceFactory factory = new EventLogTableSourceFactory();
    DynamicTableSource.Context context = createContext(options, schema);

    assertThatThrownBy(() -> factory.createDynamicTableSource(context))
        .isInstanceOf(Exception.class);
  }

  @Test
  void testCopyTableSource() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", "cassandra-event-log-connector");
    options.put("cassandra_host", "localhost");
    options.put("cassandra_user", "cassandra");
    options.put("cassandra_password", "cassandra");
    options.put("keyspace", "events");
    options.put("table", "event_log");

    ResolvedSchema schema =
        ResolvedSchema.of(
            Column.physical("bucket_hour", DataTypes.STRING()),
            Column.physical("shard", DataTypes.INT()));

    EventLogTableSourceFactory factory = new EventLogTableSourceFactory();
    DynamicTableSource.Context context = createContext(options, schema);

    EventLogTableSource source = (EventLogTableSource) factory.createDynamicTableSource(context);
    DynamicTableSource copy = source.copy();

    assertThat(copy).isInstanceOf(EventLogTableSource.class);
    assertThat(copy.asSummaryString()).isEqualTo(source.asSummaryString());
  }

  /** Helper to create a DynamicTableSource.Context for testing. */
  private DynamicTableSource.Context createContext(
      Map<String, String> options, ResolvedSchema schema) {
    return new FactoryUtil.DefaultDynamicTableContext(
        ObjectIdentifier.of("default_catalog", "default_database", "test_table"),
        new ResolvedCatalogTable(
            CatalogTable.of(
                Schema.newBuilder().fromResolvedSchema(schema).build(),
                "Test table",
                new java.util.ArrayList<>(),
                options),
            schema),
        new HashMap<>(),
        Configuration.fromMap(options),
        EventLogTableSourceFactoryTest.class.getClassLoader(),
        false);
  }
}
