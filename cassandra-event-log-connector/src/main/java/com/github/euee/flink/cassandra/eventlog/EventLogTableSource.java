package com.github.euee.flink.cassandra.eventlog;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

/**
 * Dynamic table source for Cassandra event log tables.
 *
 * <p>Provides an unbounded streaming source that reads all historical events and then continuously
 * polls for new events.
 */
public class EventLogTableSource implements ScanTableSource {

  private final EventLogConnectionConfig config;
  private final ResolvedSchema schema;

  public EventLogTableSource(EventLogConnectionConfig config, ResolvedSchema schema) {
    this.config = config;
    this.schema = schema;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // This source only produces INSERT changes (append-only)
    return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    // Get field names and types from schema
    String[] fieldNames = schema.getColumnNames().toArray(new String[0]);
    LogicalType[] fieldTypes =
        schema.getColumnDataTypes().stream()
            .map(dt -> dt.getLogicalType())
            .toArray(LogicalType[]::new);

    // Create the unbounded source
    EventLogSource source = new EventLogSource(config, fieldNames, fieldTypes);

    // Return provider that creates DataStream from source
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        return execEnv.fromSource(
            source, WatermarkStrategy.noWatermarks(), "Cassandra Event Log Source");
      }

      @Override
      public boolean isBounded() {
        return false; // Unbounded streaming source
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new EventLogTableSource(config, schema);
  }

  @Override
  public String asSummaryString() {
    return "CassandraEventLogTableSource("
        + "keyspace="
        + config.getKeyspace()
        + ", table="
        + config.getTable()
        + ", numShards="
        + config.getNumShards()
        + ", pollIntervalMs="
        + config.getPollIntervalMs()
        + ")";
  }
}
