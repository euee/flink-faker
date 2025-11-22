package com.example.connector;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ConnectorSourceFunction.
 *
 * TODO: Customize these tests for your connector:
 * 1. Test data reading/generation logic
 * 2. Test type conversion
 * 3. Test error handling
 * 4. Test resource cleanup
 */
class ConnectorSourceFunctionTest {

  // TODO: Update test schema for your connector
  private static final Schema TEST_SCHEMA =
      Schema.newBuilder()
          .column("id", DataTypes.BIGINT())
          .column("name", DataTypes.STRING())
          .column("value", DataTypes.DOUBLE())
          .build();

  @Test
  public void testSourceFunctionCreation() {
    // TODO: Test that source function can be created with valid parameters
    ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()));
    LogicalType[] types = new LogicalType[] {DataTypes.BIGINT().getLogicalType()};

    ConnectorSourceFunction sourceFunction =
        new ConnectorSourceFunction("http://example.com/api", 100, types, resolvedSchema);

    assertThat(sourceFunction).isNotNull();
  }

  @Test
  public void testLimitIsRespected() throws Exception {
    // TODO: Test that the limit parameter is properly respected
    ResolvedSchema resolvedSchema = ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()));
    LogicalType[] types = new LogicalType[] {DataTypes.BIGINT().getLogicalType()};

    ConnectorSourceFunction sourceFunction =
        new ConnectorSourceFunction("http://example.com/api", 100, types, resolvedSchema);

    sourceFunction.setLimit(5);

    // TODO: Add test logic to verify limit is respected
    // This will depend on your actual implementation
  }

  // TODO: Add more tests:
  // - Test data conversion for each supported type
  // - Test null value handling
  // - Test connection error handling
  // - Test cleanup on cancellation
  // - Test checkpoint behavior (if applicable)

  /**
   * Example test for type conversion utility
   */
  @Test
  public void testTypeConversion() {
    // TODO: Test your type conversion logic
    String[] stringValues = new String[] {"123"};
    Object result =
        TypeConversionUtils.stringValueToType(
            stringValues, DataTypes.BIGINT().getLogicalType());

    assertThat(result).isEqualTo(123L);
  }
}
