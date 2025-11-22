package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for {@link EventLogTableSource}. */
class EventLogTableSourceTest {

  private EventLogConnectionConfig config;
  private ResolvedSchema schema;

  @BeforeEach
  void setUp() {
    config =
        EventLogConnectionConfig.builder()
            .host("localhost")
            .port(9042)
            .username("cassandra")
            .password("cassandra")
            .keyspace("events")
            .table("event_log")
            .numShards(16)
            .pollIntervalMs(1000L)
            .build();

    schema =
        ResolvedSchema.of(
            Column.physical("bucket_hour", DataTypes.STRING()),
            Column.physical("shard", DataTypes.INT()),
            Column.physical("event_time", DataTypes.STRING()),
            Column.physical("event_id", DataTypes.STRING()),
            Column.physical("event_type", DataTypes.STRING()),
            Column.physical("payload", DataTypes.STRING()));
  }

  @Test
  void testChangelogMode() {
    EventLogTableSource source = new EventLogTableSource(config, schema);

    ChangelogMode changelogMode = source.getChangelogMode();

    // Event log is append-only, should only support INSERT
    assertThat(changelogMode.contains(RowKind.INSERT)).isTrue();
    assertThat(changelogMode.contains(RowKind.UPDATE_BEFORE)).isFalse();
    assertThat(changelogMode.contains(RowKind.UPDATE_AFTER)).isFalse();
    assertThat(changelogMode.contains(RowKind.DELETE)).isFalse();
  }

  @Test
  void testGetScanRuntimeProvider() {
    EventLogTableSource source = new EventLogTableSource(config, schema);

    ScanTableSource.ScanContext scanContext = new MockScanContext();
    ScanTableSource.ScanRuntimeProvider provider = source.getScanRuntimeProvider(scanContext);

    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(ScanTableSource.DataStreamScanProvider.class);

    ScanTableSource.DataStreamScanProvider streamProvider =
        (ScanTableSource.DataStreamScanProvider) provider;

    // Verify it's unbounded
    assertThat(streamProvider.isBounded()).isFalse();
  }

  @Test
  void testCopy() {
    EventLogTableSource source = new EventLogTableSource(config, schema);

    DynamicTableSource copy = source.copy();

    assertThat(copy).isNotNull();
    assertThat(copy).isInstanceOf(EventLogTableSource.class);
    assertThat(copy).isNotSameAs(source);

    EventLogTableSource copiedSource = (EventLogTableSource) copy;
    assertThat(copiedSource.asSummaryString()).isEqualTo(source.asSummaryString());
  }

  @Test
  void testAsSummaryString() {
    EventLogTableSource source = new EventLogTableSource(config, schema);

    String summary = source.asSummaryString();

    assertThat(summary).contains("CassandraEventLogTableSource");
    assertThat(summary).contains("keyspace=events");
    assertThat(summary).contains("table=event_log");
    assertThat(summary).contains("numShards=16");
    assertThat(summary).contains("pollIntervalMs=1000");
  }

  @Test
  void testWithDifferentConfiguration() {
    EventLogConnectionConfig customConfig =
        EventLogConnectionConfig.builder()
            .host("custom-host")
            .port(9043)
            .username("user")
            .password("pass")
            .keyspace("custom_ks")
            .table("custom_table")
            .numShards(32)
            .pollIntervalMs(2000L)
            .build();

    EventLogTableSource source = new EventLogTableSource(customConfig, schema);

    String summary = source.asSummaryString();

    assertThat(summary).contains("keyspace=custom_ks");
    assertThat(summary).contains("table=custom_table");
    assertThat(summary).contains("numShards=32");
    assertThat(summary).contains("pollIntervalMs=2000");
  }

  @Test
  void testWithMinimalSchema() {
    ResolvedSchema minimalSchema =
        ResolvedSchema.of(
            Column.physical("bucket_hour", DataTypes.STRING()),
            Column.physical("event_time", DataTypes.STRING()));

    EventLogTableSource source = new EventLogTableSource(config, minimalSchema);

    assertThat(source).isNotNull();
    assertThat(source.asSummaryString()).contains("CassandraEventLogTableSource");
  }

  /** Mock implementation of ScanContext for testing. */
  private static class MockScanContext implements ScanTableSource.ScanContext {
    @Override
    public <T> org.apache.flink.table.types.inference.TypeInference createTypeInference(
        org.apache.flink.table.catalog.DataTypeFactory dataTypeFactory) {
      return null;
    }

    @Override
    public org.apache.flink.table.catalog.DataTypeFactory createDataTypeFactory() {
      return null;
    }
  }
}
