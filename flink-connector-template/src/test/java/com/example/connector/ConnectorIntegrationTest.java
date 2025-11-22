package com.example.connector;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {{CONNECTOR_NAME}} connector.
 *
 * TODO: Customize these tests for your connector:
 * 1. Update DDL statements with your connector's configuration
 * 2. Add tests for all supported data types
 * 3. Test various connector configurations
 * 4. Test error scenarios
 * 5. Consider adding tests with actual external resources (if applicable)
 */
public class ConnectorIntegrationTest {

  @Test
  public void testBasicSourceFunctionality() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // TODO: Update DDL with your connector's configuration
    tEnv.executeSql(
        "CREATE TABLE test_table (\n"
            + "    id BIGINT,\n"
            + "    name STRING,\n"
            + "    value DOUBLE\n"
            + ") WITH (\n"
            + "  'connector' = '{{CONNECTOR_NAME}}',\n"
            + "  'endpoint' = 'http://example.com/api',\n"
            + "  'batch-size' = '10'\n"
            + ")");

    // TODO: Add assertions based on your connector's behavior
    TableResult tableResult = tEnv.executeSql("SELECT * FROM test_table LIMIT 5");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      Row row = collect.next();
      numRows++;
      // TODO: Add assertions on row content
      assertThat(row).isNotNull();
    }

    // Verify we got the expected number of rows
    assertThat(numRows).isEqualTo(5);
  }

  @Test
  public void testAllSupportedDataTypes() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // TODO: Create table with all data types your connector supports
    tEnv.executeSql(
        "CREATE TABLE all_types (\n"
            + "  f_tinyint TINYINT,\n"
            + "  f_smallint SMALLINT,\n"
            + "  f_int INT,\n"
            + "  f_bigint BIGINT,\n"
            + "  f_float FLOAT,\n"
            + "  f_double DOUBLE,\n"
            + "  f_decimal DECIMAL(10,2),\n"
            + "  f_char CHAR(10),\n"
            + "  f_varchar VARCHAR(255),\n"
            + "  f_string STRING,\n"
            + "  f_boolean BOOLEAN,\n"
            + "  f_date DATE,\n"
            + "  f_timestamp TIMESTAMP(3)\n"
            // TODO: Add ARRAY, MAP, ROW types if supported
            + ") WITH (\n"
            + "  'connector' = '{{CONNECTOR_NAME}}',\n"
            + "  'endpoint' = 'http://example.com/api'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM all_types LIMIT 3");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      Row row = collect.next();
      numRows++;
      // TODO: Verify each field type and value
      assertThat(row.getArity()).isEqualTo(13);
    }

    assertThat(numRows).isEqualTo(3);
  }

  @Test
  public void testWithComputedColumn() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // TODO: Test computed columns work with your connector
    tEnv.executeSql(
        "CREATE TABLE test_computed (\n"
            + "    id BIGINT,\n"
            + "    value DOUBLE,\n"
            + "    computed_time AS PROCTIME()\n"
            + ") WITH (\n"
            + "  'connector' = '{{CONNECTOR_NAME}}',\n"
            + "  'endpoint' = 'http://example.com/api'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM test_computed LIMIT 2");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      collect.next();
      numRows++;
    }

    assertThat(numRows).isEqualTo(2);
  }

  @Test
  public void testLimitPushDown() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // TODO: Test that limit pushdown works correctly
    tEnv.executeSql(
        "CREATE TABLE test_limit (\n"
            + "    id BIGINT\n"
            + ") WITH (\n"
            + "  'connector' = '{{CONNECTOR_NAME}}',\n"
            + "  'endpoint' = 'http://example.com/api'\n"
            + ")");

    TableResult tableResult = tEnv.executeSql("SELECT * FROM test_limit LIMIT 10");

    CloseableIterator<Row> collect = tableResult.collect();

    int numRows = 0;
    while (collect.hasNext()) {
      collect.next();
      numRows++;
    }

    assertThat(numRows).isEqualTo(10);
  }

  // TODO: Add more integration tests:
  // - Test with different parallelism settings
  // - Test error handling (invalid endpoint, connection failures, etc.)
  // - Test with filters (if filter pushdown is supported)
  // - Test with projections (if projection pushdown is supported)
  // - Test lookup joins (if LookupTableSource is implemented)
  // - Test watermark strategies (for event-time processing)
  // - Test checkpointing and recovery
}
