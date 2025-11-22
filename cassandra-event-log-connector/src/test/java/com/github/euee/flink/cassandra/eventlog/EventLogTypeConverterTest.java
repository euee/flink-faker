package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.time.Instant;
import java.util.UUID;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link EventLogTypeConverter}. */
class EventLogTypeConverterTest {

  @Test
  void testConvertStringField() {
    Row row = mock(Row.class);
    when(row.isNull("field1")).thenReturn(false);
    when(row.getString("field1")).thenReturn("test-value");

    String[] fieldNames = {"field1"};
    LogicalType[] types = {new VarCharType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.getArity()).isEqualTo(1);
    assertThat(result.getString(0).toString()).isEqualTo("test-value");
  }

  @Test
  void testConvertUUIDToString() {
    Row row = mock(Row.class);
    UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

    when(row.isNull("event_id")).thenReturn(false);
    when(row.getString("event_id")).thenThrow(new IllegalArgumentException("Not a string"));
    when(row.getUuid("event_id")).thenReturn(uuid);

    String[] fieldNames = {"event_id"};
    LogicalType[] types = {new VarCharType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.getString(0).toString()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
  }

  @Test
  void testConvertIntField() {
    Row row = mock(Row.class);
    when(row.isNull("shard")).thenReturn(false);
    when(row.getInt("shard")).thenReturn(5);

    String[] fieldNames = {"shard"};
    LogicalType[] types = {new IntType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.getInt(0)).isEqualTo(5);
  }

  @Test
  void testConvertLongField() {
    Row row = mock(Row.class);
    when(row.isNull("timestamp")).thenReturn(false);
    when(row.getLong("timestamp")).thenReturn(1234567890L);

    String[] fieldNames = {"timestamp"};
    LogicalType[] types = {new BigIntType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.getLong(0)).isEqualTo(1234567890L);
  }

  @Test
  void testConvertBooleanField() {
    Row row = mock(Row.class);
    when(row.isNull("is_active")).thenReturn(false);
    when(row.getBoolean("is_active")).thenReturn(true);

    String[] fieldNames = {"is_active"};
    LogicalType[] types = {new BooleanType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.getBoolean(0)).isTrue();
  }

  @Test
  void testConvertDoubleField() {
    Row row = mock(Row.class);
    when(row.isNull("value")).thenReturn(false);
    when(row.getDouble("value")).thenReturn(123.456);

    String[] fieldNames = {"value"};
    LogicalType[] types = {new DoubleType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.getDouble(0)).isEqualTo(123.456);
  }

  @Test
  void testConvertNullField() {
    Row row = mock(Row.class);
    when(row.isNull("nullable_field")).thenReturn(true);

    String[] fieldNames = {"nullable_field"};
    LogicalType[] types = {new VarCharType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.isNullAt(0)).isTrue();
  }

  @Test
  void testConvertTimestampFromInstant() {
    Row row = mock(Row.class);
    Instant instant = Instant.ofEpochMilli(1700000000000L);

    when(row.isNull("created_at")).thenReturn(false);
    when(row.getInstant("created_at")).thenReturn(instant);

    String[] fieldNames = {"created_at"};
    LogicalType[] types = {new TimestampType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    TimestampData timestamp = result.getTimestamp(0, 6);
    assertThat(timestamp.getMillisecond()).isEqualTo(1700000000000L);
  }

  @Test
  void testConvertTimestampFromTimeUUID() {
    Row row = mock(Row.class);

    // Create a TimeUUID (version 1)
    UUID timeUuid = Uuids.timeBased();

    when(row.isNull("event_time")).thenReturn(false);
    when(row.getInstant("event_time")).thenThrow(new IllegalArgumentException("Not an instant"));
    when(row.getUuid("event_time")).thenReturn(timeUuid);

    String[] fieldNames = {"event_time"};
    LogicalType[] types = {new TimestampType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    TimestampData timestamp = result.getTimestamp(0, 6);
    assertThat(timestamp).isNotNull();
    assertThat(timestamp.getMillisecond()).isGreaterThan(0);
  }

  @Test
  void testConvertMultipleFields() {
    Row row = mock(Row.class);
    UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

    when(row.isNull("bucket_hour")).thenReturn(false);
    when(row.getString("bucket_hour")).thenReturn("2025-11-22-14");

    when(row.isNull("shard")).thenReturn(false);
    when(row.getInt("shard")).thenReturn(7);

    when(row.isNull("event_time")).thenReturn(false);
    when(row.getString("event_time")).thenThrow(new IllegalArgumentException());
    when(row.getUuid("event_time")).thenReturn(uuid);

    when(row.isNull("payload")).thenReturn(false);
    when(row.getString("payload")).thenReturn("{\"key\":\"value\"}");

    String[] fieldNames = {"bucket_hour", "shard", "event_time", "payload"};
    LogicalType[] types = {new VarCharType(), new IntType(), new VarCharType(), new VarCharType()};

    RowData result = EventLogTypeConverter.convertRow(row, fieldNames, types);

    assertThat(result.getArity()).isEqualTo(4);
    assertThat(result.getString(0).toString()).isEqualTo("2025-11-22-14");
    assertThat(result.getInt(1)).isEqualTo(7);
    assertThat(result.getString(2).toString()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    assertThat(result.getString(3).toString()).isEqualTo("{\"key\":\"value\"}");
  }
}
