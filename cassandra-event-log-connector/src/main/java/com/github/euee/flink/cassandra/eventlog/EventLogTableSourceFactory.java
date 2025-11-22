package com.github.euee.flink.cassandra.eventlog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import java.util.HashSet;
import java.util.Set;

/** Factory for creating EventLogTableSource instances. */
public class EventLogTableSourceFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "cassandra-event-log-connector";

  // Required Cassandra connection options
  public static final ConfigOption<String> CASSANDRA_HOST =
      ConfigOptions.key("cassandra_host")
          .stringType()
          .noDefaultValue()
          .withDescription("Cassandra contact point host");

  public static final ConfigOption<Integer> CASSANDRA_PORT =
      ConfigOptions.key("cassandra_port")
          .intType()
          .defaultValue(9042)
          .withDescription("Cassandra contact point port");

  public static final ConfigOption<String> CASSANDRA_USER =
      ConfigOptions.key("cassandra_user")
          .stringType()
          .noDefaultValue()
          .withDescription("Username for Cassandra authentication");

  public static final ConfigOption<String> CASSANDRA_PASSWORD =
      ConfigOptions.key("cassandra_password")
          .stringType()
          .noDefaultValue()
          .withDescription("Password for Cassandra authentication");

  public static final ConfigOption<String> KEYSPACE =
      ConfigOptions.key("keyspace")
          .stringType()
          .noDefaultValue()
          .withDescription("Cassandra keyspace name");

  public static final ConfigOption<String> TABLE =
      ConfigOptions.key("table")
          .stringType()
          .noDefaultValue()
          .withDescription("Cassandra event log table name");

  public static final ConfigOption<String> DATACENTER =
      ConfigOptions.key("datacenter")
          .stringType()
          .defaultValue("datacenter1")
          .withDescription("Cassandra datacenter name");

  // Event log specific options
  public static final ConfigOption<Integer> NUM_SHARDS =
      ConfigOptions.key("num-shards")
          .intType()
          .defaultValue(16)
          .withDescription("Number of shards in the event log table (0 to num-shards-1)");

  public static final ConfigOption<Long> POLL_INTERVAL_MS =
      ConfigOptions.key("poll-interval-ms")
          .longType()
          .defaultValue(1000L)
          .withDescription("Polling interval in milliseconds for checking new events");

  public static final ConfigOption<Integer> FETCH_SIZE =
      ConfigOptions.key("fetch-size")
          .intType()
          .defaultValue(5000)
          .withDescription("Number of rows to fetch per Cassandra query");

  public static final ConfigOption<Integer> LOOKBACK_HOURS =
      ConfigOptions.key("lookback-hours")
          .intType()
          .defaultValue(1)
          .withDescription(
              "Number of hours to look back when querying buckets (to catch late events)");

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
    options.add(NUM_SHARDS);
    options.add(POLL_INTERVAL_MS);
    options.add(FETCH_SIZE);
    options.add(LOOKBACK_HOURS);
    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);

    helper.validate();

    // Get configuration
    EventLogConnectionConfig config =
        EventLogConnectionConfig.builder()
            .host(helper.getOptions().get(CASSANDRA_HOST))
            .port(helper.getOptions().get(CASSANDRA_PORT))
            .username(helper.getOptions().get(CASSANDRA_USER))
            .password(helper.getOptions().get(CASSANDRA_PASSWORD))
            .keyspace(helper.getOptions().get(KEYSPACE))
            .table(helper.getOptions().get(TABLE))
            .datacenter(helper.getOptions().get(DATACENTER))
            .numShards(helper.getOptions().get(NUM_SHARDS))
            .pollIntervalMs(helper.getOptions().get(POLL_INTERVAL_MS))
            .fetchSize(helper.getOptions().get(FETCH_SIZE))
            .lookbackHours(helper.getOptions().get(LOOKBACK_HOURS))
            .build();

    // Get table schema
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

    return new EventLogTableSource(config, schema);
  }
}
