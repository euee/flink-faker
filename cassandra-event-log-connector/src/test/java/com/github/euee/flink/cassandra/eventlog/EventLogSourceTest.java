package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for {@link EventLogSource}. */
class EventLogSourceTest {

  private EventLogConnectionConfig config;
  private String[] fieldNames;
  private LogicalType[] fieldTypes;

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
            .datacenter("dc1")
            .numShards(16)
            .pollIntervalMs(1000L)
            .fetchSize(5000)
            .lookbackHours(1)
            .build();

    fieldNames = new String[] {"bucket_hour", "shard", "event_time", "payload"};
    fieldTypes =
        new LogicalType[] {new VarCharType(), new IntType(), new VarCharType(), new VarCharType()};
  }

  @Test
  void testSourceBoundedness() {
    EventLogSource source = new EventLogSource(config, fieldNames, fieldTypes);

    assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Test
  void testCreateEnumerator() throws Exception {
    EventLogSource source = new EventLogSource(config, fieldNames, fieldTypes);

    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);

    var enumerator = source.createEnumerator(context);

    assertThat(enumerator).isNotNull();
    assertThat(enumerator).isInstanceOf(EventLogSplitEnumerator.class);
  }

  @Test
  void testRestoreEnumerator() throws Exception {
    EventLogSource source = new EventLogSource(config, fieldNames, fieldTypes);

    @SuppressWarnings("unchecked")
    SplitEnumeratorContext<EventLogSourceSplit> context = mock(SplitEnumeratorContext.class);

    // Create checkpoint state
    java.util.List<EventLogSourceSplit> splits =
        java.util.Arrays.asList(
            new EventLogSourceSplit(0, "uuid-0"), new EventLogSourceSplit(1, "uuid-1"));
    EventLogEnumeratorState checkpoint = new EventLogEnumeratorState(splits);

    var enumerator = source.restoreEnumerator(context, checkpoint);

    assertThat(enumerator).isNotNull();
    assertThat(enumerator).isInstanceOf(EventLogSplitEnumerator.class);
  }

  @Test
  void testGetSplitSerializer() {
    EventLogSource source = new EventLogSource(config, fieldNames, fieldTypes);

    SimpleVersionedSerializer<EventLogSourceSplit> serializer = source.getSplitSerializer();

    assertThat(serializer).isNotNull();
    assertThat(serializer).isInstanceOf(EventLogSourceSplitSerializer.class);
  }

  @Test
  void testGetEnumeratorCheckpointSerializer() {
    EventLogSource source = new EventLogSource(config, fieldNames, fieldTypes);

    SimpleVersionedSerializer<EventLogEnumeratorState> serializer =
        source.getEnumeratorCheckpointSerializer();

    assertThat(serializer).isNotNull();
    assertThat(serializer).isInstanceOf(EventLogEnumeratorStateSerializer.class);
  }

  @Test
  void testSourceWithDifferentConfiguration() {
    EventLogConnectionConfig customConfig =
        EventLogConnectionConfig.builder()
            .host("custom-host")
            .port(9043)
            .username("user")
            .password("pass")
            .keyspace("ks")
            .table("tbl")
            .numShards(32)
            .pollIntervalMs(2000L)
            .build();

    EventLogSource source = new EventLogSource(customConfig, fieldNames, fieldTypes);

    assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Test
  void testSourceWithMinimalFields() {
    String[] minimalFieldNames = new String[] {"event_time"};
    LogicalType[] minimalFieldTypes = new LogicalType[] {new VarCharType()};

    EventLogSource source = new EventLogSource(config, minimalFieldNames, minimalFieldTypes);

    assertThat(source).isNotNull();
    assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Test
  void testSourceWithManyFields() {
    String[] manyFieldNames =
        new String[] {
          "bucket_hour",
          "shard",
          "event_time",
          "event_id",
          "event_type",
          "user_id",
          "session_id",
          "payload",
          "created_at"
        };
    LogicalType[] manyFieldTypes =
        new LogicalType[] {
          new VarCharType(),
          new IntType(),
          new VarCharType(),
          new VarCharType(),
          new VarCharType(),
          new VarCharType(),
          new VarCharType(),
          new VarCharType(),
          new VarCharType()
        };

    EventLogSource source = new EventLogSource(config, manyFieldNames, manyFieldTypes);

    assertThat(source).isNotNull();
  }
}
