package com.example.connector;

import static org.apache.flink.configuration.ConfigOptions.key;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;

/**
 * Factory for creating {{CONNECTOR_NAME}} table sources.
 *
 * TODO: Customize this factory for your connector:
 * 1. Update IDENTIFIER to your connector name
 * 2. Add connector-specific configuration options
 * 3. Implement validation logic for your connector
 * 4. Update createDynamicTableSource to pass required parameters to TableSource
 */
public class ConnectorTableSourceFactory implements DynamicTableSourceFactory {

  // TODO: Replace with your connector identifier (e.g., "kafka", "jdbc", "http")
  public static final String IDENTIFIER = "{{CONNECTOR_NAME}}";

  // TODO: Define your connector-specific configuration options
  // Example configuration options:
  public static final ConfigOption<String> ENDPOINT =
      key("endpoint")
          .stringType()
          .noDefaultValue()
          .withDescription("The endpoint URL for the connector");

  public static final ConfigOption<Integer> BATCH_SIZE =
      key("batch-size")
          .intType()
          .defaultValue(100)
          .withDescription("Number of records to fetch in each batch");

  @Override
  public ConnectorTableSource createDynamicTableSource(final Context context) {

    Configuration options = new Configuration();
    context.getCatalogTable().getOptions().forEach(options::setString);

    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    List<Column> physicalColumns =
        schema.getColumns().stream()
            .filter(column -> column.isPhysical())
            .collect(Collectors.toList());

    // TODO: Extract and validate connector-specific options
    String endpoint = options.get(ENDPOINT);
    Integer batchSize = options.get(BATCH_SIZE);

    // TODO: Validate schema and options
    validateConfiguration(options, schema);

    // TODO: Return your TableSource implementation with required parameters
    return new ConnectorTableSource(schema, endpoint, batchSize);
  }

  /**
   * Validate configuration options and schema.
   *
   * TODO: Add your connector-specific validation logic
   */
  private void validateConfiguration(Configuration options, ResolvedSchema schema) {
    // Example validation
    if (!options.contains(ENDPOINT)) {
      throw new ValidationException("Required option 'endpoint' is not specified.");
    }

    // TODO: Add schema validation, data type validation, etc.
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    // TODO: Return set of required configuration options
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(ENDPOINT);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    // TODO: Return set of optional configuration options
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(BATCH_SIZE);
    return options;
  }
}
