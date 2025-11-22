package com.github.euee.flink.cassandra;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

/** Table source implementation for Cassandra connector. */
public class CassandraTableSource implements ScanTableSource {

  private final ResolvedSchema schema;
  private final LogicalType[] types;
  private final String[] fieldNames;
  private final CassandraConnectionConfig connectionConfig;
  private final String keyspace;
  private final String table;
  private final int fetchSize;
  private final int parallelism;

  public CassandraTableSource(
      ResolvedSchema schema,
      CassandraConnectionConfig connectionConfig,
      String keyspace,
      String table,
      int fetchSize,
      int parallelism) {
    this.schema = schema;
    this.connectionConfig = connectionConfig;
    this.keyspace = keyspace;
    this.table = table;
    this.fetchSize = fetchSize;
    this.parallelism = parallelism;

    this.types =
        schema.getColumns().stream()
            .filter(column -> column.isPhysical())
            .map(Column::getDataType)
            .map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);

    this.fieldNames =
        schema.getColumns().stream()
            .filter(column -> column.isPhysical())
            .map(Column::getName)
            .toArray(String[]::new);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // Cassandra table source is append-only (read-only)
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment env) {

        CassandraSource source =
            new CassandraSource(
                connectionConfig, keyspace, table, fieldNames, types, fetchSize, parallelism);

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Cassandra Source")
            .returns(
                providerContext
                    .createTypeInformation(schema.toPhysicalRowDataType())
                    .getTypeClass());
      }

      @Override
      public boolean isBounded() {
        // Cassandra table source is bounded (reads existing data)
        return true;
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new CassandraTableSource(
        schema, connectionConfig, keyspace, table, fetchSize, parallelism);
  }

  @Override
  public String asSummaryString() {
    return "CassandraSource(keyspace=" + keyspace + ", table=" + table + ")";
  }
}
