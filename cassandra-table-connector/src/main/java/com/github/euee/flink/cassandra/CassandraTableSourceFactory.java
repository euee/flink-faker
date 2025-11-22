package com.github.euee.flink.cassandra;

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

/** Factory for creating Cassandra table sources. */
public class CassandraTableSourceFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "cassandra-table-connector";

  // Cassandra connection configuration
  public static final ConfigOption<String> CASSANDRA_HOST =
      key("cassandra_host")
          .stringType()
          .noDefaultValue()
          .withDescription("Cassandra contact point host");

  public static final ConfigOption<Integer> CASSANDRA_PORT =
      key("cassandra_port")
          .intType()
          .defaultValue(9042)
          .withDescription("Cassandra contact point port");

  public static final ConfigOption<String> CASSANDRA_USER =
      key("cassandra_user")
          .stringType()
          .noDefaultValue()
          .withDescription("Cassandra username for authentication");

  public static final ConfigOption<String> CASSANDRA_PASSWORD =
      key("cassandra_password")
          .stringType()
          .noDefaultValue()
          .withDescription("Cassandra password for authentication");

  public static final ConfigOption<String> KEYSPACE =
      key("keyspace").stringType().noDefaultValue().withDescription("Cassandra keyspace name");

  public static final ConfigOption<String> TABLE =
      key("table").stringType().noDefaultValue().withDescription("Cassandra table name");

  // Optional configuration
  public static final ConfigOption<String> DATACENTER =
      key("datacenter")
          .stringType()
          .defaultValue("datacenter1")
          .withDescription("Cassandra datacenter name");

  public static final ConfigOption<Integer> FETCH_SIZE =
      key("fetch-size")
          .intType()
          .defaultValue(5000)
          .withDescription("Number of rows to fetch per request");

  public static final ConfigOption<Integer> PARALLELISM =
      key("parallelism")
          .intType()
          .defaultValue(1)
          .withDescription("Number of parallel readers");

  @Override
  public CassandraTableSource createDynamicTableSource(final Context context) {

    Configuration options = new Configuration();
    context.getCatalogTable().getOptions().forEach(options::setString);

    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    List<Column> physicalColumns =
        schema.getColumns().stream()
            .filter(column -> column.isPhysical())
            .collect(Collectors.toList());

    // Validate required options
    validateConfiguration(options, schema);

    // Extract configuration
    String host = options.get(CASSANDRA_HOST);
    Integer port = options.get(CASSANDRA_PORT);
    String user = options.get(CASSANDRA_USER);
    String password = options.get(CASSANDRA_PASSWORD);
    String keyspace = options.get(KEYSPACE);
    String table = options.get(TABLE);
    String datacenter = options.get(DATACENTER);
    Integer fetchSize = options.get(FETCH_SIZE);
    Integer parallelism = options.get(PARALLELISM);

    CassandraConnectionConfig connectionConfig =
        new CassandraConnectionConfig(host, port, user, password, datacenter);

    return new CassandraTableSource(
        schema, connectionConfig, keyspace, table, fetchSize, parallelism);
  }

  private void validateConfiguration(Configuration options, ResolvedSchema schema) {
    // Validate required Cassandra connection options
    if (!options.contains(CASSANDRA_HOST)) {
      throw new ValidationException("Required option 'cassandra_host' is not specified.");
    }

    if (!options.contains(CASSANDRA_USER)) {
      throw new ValidationException("Required option 'cassandra_user' is not specified.");
    }

    if (!options.contains(CASSANDRA_PASSWORD)) {
      throw new ValidationException("Required option 'cassandra_password' is not specified.");
    }

    if (!options.contains(KEYSPACE)) {
      throw new ValidationException("Required option 'keyspace' is not specified.");
    }

    if (!options.contains(TABLE)) {
      throw new ValidationException("Required option 'table' is not specified.");
    }

    // Validate fetch size
    Integer fetchSize = options.get(FETCH_SIZE);
    if (fetchSize <= 0) {
      throw new ValidationException(
          "Fetch size must be positive. Got: " + fetchSize);
    }

    // Validate parallelism
    Integer parallelism = options.get(PARALLELISM);
    if (parallelism <= 0) {
      throw new ValidationException(
          "Parallelism must be positive. Got: " + parallelism);
    }
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(CASSANDRA_HOST);
    options.add(CASSANDRA_USER);
    options.add(CASSANDRA_PASSWORD);
    options.add(KEYSPACE);
    options.add(TABLE);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(CASSANDRA_PORT);
    options.add(DATACENTER);
    options.add(FETCH_SIZE);
    options.add(PARALLELISM);
    return options;
  }
}
