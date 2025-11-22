package com.github.euee.flink.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UuidData;
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

/** Utility class for converting Cassandra Row to Flink RowData. */
public class CassandraTypeConverter {

  /**
   * Convert a Cassandra Row to Flink RowData.
   *
   * @param cassandraRow The Cassandra row
   * @param fieldNames The Flink field names (mapped to Cassandra columns)
   * @param types The Flink logical types
   * @return Flink RowData
   */
  public static RowData convertRow(Row cassandraRow, String[] fieldNames, LogicalType[] types) {
    GenericRowData row = new GenericRowData(fieldNames.length);

    for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      LogicalType type = types[i];

      // Convert Cassandra column name (e.g., user_id) to match Flink schema
      // In this implementation, we assume column names match exactly
      // You can add custom mapping logic here

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
          Instant instant = cassandraRow.getInstant(columnName);
          if (instant != null) {
            return TimestampData.fromInstant(instant);
          }
          return null;

        case DECIMAL:
          java.math.BigDecimal decimal = cassandraRow.getBigDecimal(columnName);
          if (decimal != null) {
            return org.apache.flink.table.data.DecimalData.fromBigDecimal(
                decimal,
                logicalType.asSerializableString().length(),
                ((org.apache.flink.table.types.logical.DecimalType) logicalType).getScale());
          }
          return null;

        // Note: For complex types (ARRAY, MAP, ROW), you'd need additional conversion logic
        // This basic implementation focuses on primitive types

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
      // Try UUID first (common in Cassandra)
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

  /**
   * Map Cassandra column name to Flink field name.
   *
   * This can be customized to handle naming conventions, e.g.:
   * - Cassandra: userid -> Flink: user_id
   * - Cassandra: feedbackid -> Flink: feedback_id
   *
   * @param cassandraColumnName The Cassandra column name
   * @return The mapped Flink field name
   */
  public static String mapColumnName(String cassandraColumnName) {
    // Example: Convert camelCase to snake_case
    // This is a simplified implementation
    return cassandraColumnName;
  }
}
