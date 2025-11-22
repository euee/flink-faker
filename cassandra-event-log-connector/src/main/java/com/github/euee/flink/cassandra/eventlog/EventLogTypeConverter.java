package com.github.euee.flink.cassandra.eventlog;

import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Utility class for converting Cassandra event log Row to Flink RowData.
 *
 * <p>Handles special conversion for TimeUUID fields used in event logs.
 */
public class EventLogTypeConverter {

  /**
   * Convert a Cassandra Row to Flink RowData.
   *
   * @param cassandraRow The Cassandra row
   * @param fieldNames The Flink field names
   * @param types The Flink logical types
   * @return Flink RowData
   */
  public static RowData convertRow(Row cassandraRow, String[] fieldNames, LogicalType[] types) {
    GenericRowData row = new GenericRowData(fieldNames.length);

    for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      LogicalType type = types[i];

      Object value = convertField(cassandraRow, fieldName, type);
      row.setField(i, value);
    }

    return row;
  }

  private static Object convertField(Row cassandraRow, String columnName, LogicalType logicalType) {
    // Handle null values
    if (cassandraRow.isNull(columnName)) {
      return null;
    }

    try {
      switch (logicalType.getTypeRoot()) {
        case CHAR:
        case VARCHAR:
          return convertToString(cassandraRow, columnName);

        case BOOLEAN:
          return cassandraRow.getBoolean(columnName);

        case TINYINT:
          return cassandraRow.getByte(columnName);

        case SMALLINT:
          return cassandraRow.getShort(columnName);

        case INTEGER:
          return cassandraRow.getInt(columnName);

        case BIGINT:
          return cassandraRow.getLong(columnName);

        case FLOAT:
          return cassandraRow.getFloat(columnName);

        case DOUBLE:
          return cassandraRow.getDouble(columnName);

        case DATE:
          LocalDate localDate = cassandraRow.getLocalDate(columnName);
          if (localDate != null) {
            return (int) localDate.toEpochDay();
          }
          return null;

        case TIME_WITHOUT_TIME_ZONE:
          LocalTime localTime = cassandraRow.getLocalTime(columnName);
          if (localTime != null) {
            return (int) (localTime.toNanoOfDay() / 1_000_000);
          }
          return null;

        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          // Handle both Instant and TimeUUID
          return convertToTimestamp(cassandraRow, columnName);

        case DECIMAL:
          java.math.BigDecimal decimal = cassandraRow.getBigDecimal(columnName);
          if (decimal != null) {
            return org.apache.flink.table.data.DecimalData.fromBigDecimal(
                decimal,
                logicalType.asSerializableString().length(),
                ((org.apache.flink.table.types.logical.DecimalType) logicalType).getScale());
          }
          return null;

        default:
          // For unsupported types, try to convert to string
          return convertToString(cassandraRow, columnName);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to convert Cassandra column '"
              + columnName
              + "' to Flink type "
              + logicalType.getTypeRoot(),
          e);
    }
  }

  private static StringData convertToString(Row cassandraRow, String columnName) {
    // Handle different Cassandra types that can be converted to string
    try {
      // Try UUID first (common in Cassandra, including TimeUUID)
      UUID uuid = cassandraRow.getUuid(columnName);
      if (uuid != null) {
        return StringData.fromString(uuid.toString());
      }
    } catch (Exception e) {
      // Not a UUID, try string
    }

    try {
      String value = cassandraRow.getString(columnName);
      if (value != null) {
        return StringData.fromString(value);
      }
    } catch (Exception e) {
      // Not a string
    }

    // Try to get as object and convert to string
    Object obj = cassandraRow.getObject(columnName);
    if (obj != null) {
      return StringData.fromString(obj.toString());
    }

    return null;
  }

  private static TimestampData convertToTimestamp(Row cassandraRow, String columnName) {
    // Try Instant first
    try {
      Instant instant = cassandraRow.getInstant(columnName);
      if (instant != null) {
        return TimestampData.fromInstant(instant);
      }
    } catch (Exception e) {
      // Not an Instant
    }

    // Try TimeUUID - extract timestamp from UUID version 1
    try {
      UUID uuid = cassandraRow.getUuid(columnName);
      if (uuid != null && uuid.version() == 1) {
        // TimeUUID (version 1) contains timestamp
        // Extract timestamp from TimeUUID
        long timestamp = getTimestampFromTimeUuid(uuid);
        return TimestampData.fromEpochMillis(timestamp);
      }
    } catch (Exception e) {
      // Not a TimeUUID
    }

    return null;
  }

  /**
   * Extract timestamp (milliseconds since epoch) from TimeUUID.
   *
   * <p>TimeUUID (UUID v1) stores timestamp as 100-nanosecond intervals since UUID epoch
   * (1582-10-15 00:00:00).
   */
  private static long getTimestampFromTimeUuid(UUID uuid) {
    // UUID v1 timestamp is stored across time_low, time_mid, and time_hi fields
    long timestamp = uuid.timestamp();

    // Convert from 100-nanosecond intervals since UUID epoch to milliseconds since Unix epoch
    // UUID epoch: 1582-10-15 00:00:00
    // Unix epoch: 1970-01-01 00:00:00
    // Difference: 122192928000000000 100-nanosecond intervals
    final long UUID_EPOCH_OFFSET = 122192928000000000L;

    long uuidTime = timestamp;
    long unixTime100Nanos = uuidTime - UUID_EPOCH_OFFSET;
    long unixTimeMillis = unixTime100Nanos / 10000; // Convert to milliseconds

    return unixTimeMillis;
  }
}
