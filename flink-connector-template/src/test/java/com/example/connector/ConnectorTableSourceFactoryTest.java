package com.example.connector;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ConnectorTableSourceFactory.
 *
 * TODO: Customize these tests for your connector:
 * 1. Update test schemas to match your supported data types
 * 2. Add tests for your connector-specific configuration options
 * 3. Add validation tests for invalid configurations
 * 4. Test edge cases specific to your connector
 */
class ConnectorTableSourceFactoryTest {

  // TODO: Define test schemas that cover your supported data types
  private static final Schema VALID_SCHEMA =
      Schema.newBuilder()
          .column("id", DataTypes.BIGINT())
          .column("name", DataTypes.STRING())
          .column("value", DataTypes.DOUBLE())
          .column("timestamp", DataTypes.TIMESTAMP())
          .build();

  private static final Schema INVALID_SCHEMA =
      Schema.newBuilder()
          .column("id", DataTypes.BIGINT())
          .column("invalid_type", DataTypes.NULL())
          .build();

  @Test
  public void testValidTableSourceIsValid() {
    // TODO: Update with valid configuration for your connector
    Map<String, String> properties = new HashMap<>();
    properties.put(FactoryUtil.CONNECTOR.key(), "{{CONNECTOR_NAME}}");
    properties.put("endpoint", "http://example.com/api");
    properties.put("batch-size", "100");

    DynamicTableSource tableSource = createTableSource(properties, VALID_SCHEMA);
    assertThat(tableSource).isNotNull();
    assertThat(tableSource).isInstanceOf(ConnectorTableSource.class);
  }

  @Test
  public void testMissingRequiredOptionIsInvalid() {
    // TODO: Test that missing required options throw ValidationException
    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap<>();
              properties.put(FactoryUtil.CONNECTOR.key(), "{{CONNECTOR_NAME}}");
              // Missing required 'endpoint' option

              createTableSource(properties, VALID_SCHEMA);
            })
        .withStackTraceContaining("endpoint");
  }

  @Test
  public void testInvalidSchemaIsInvalid() {
    // TODO: Test that invalid schemas throw ValidationException
    assertThatExceptionOfType(ValidationException.class)
        .isThrownBy(
            () -> {
              Map<String, String> properties = new HashMap<>();
              properties.put(FactoryUtil.CONNECTOR.key(), "{{CONNECTOR_NAME}}");
              properties.put("endpoint", "http://example.com/api");

              createTableSource(properties, INVALID_SCHEMA);
            });
  }

  @Test
  public void testOptionalParametersWork() {
    // TODO: Test that optional parameters are properly handled
    Map<String, String> properties = new HashMap<>();
    properties.put(FactoryUtil.CONNECTOR.key(), "{{CONNECTOR_NAME}}");
    properties.put("endpoint", "http://example.com/api");
    // Test with non-default batch size
    properties.put("batch-size", "500");

    DynamicTableSource tableSource = createTableSource(properties, VALID_SCHEMA);
    assertThat(tableSource).isNotNull();
  }

  // TODO: Add more connector-specific tests
  // Examples:
  // - Test authentication configuration
  // - Test timeout settings
  // - Test format options
  // - Test partition configuration
  // - Test filter pushdown capabilities

  private DynamicTableSource createTableSource(Map<String, String> properties, Schema schema) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;

    CatalogTable table = CatalogTable.of(schema, "comment", Arrays.asList(), properties);

    return FactoryUtil.createDynamicTableSource(
        null,
        ObjectIdentifier.of("", "", ""),
        tableEnvInternal.getCatalogManager().resolveCatalogTable(table),
        new HashMap<>(),
        new Configuration(),
        Thread.currentThread().getContextClassLoader(),
        false);
  }
}
