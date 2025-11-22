package com.example.connector;

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
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Table source implementation for {{CONNECTOR_NAME}} connector.
 *
 * TODO: Customize this table source for your connector:
 * 1. Implement appropriate interfaces (ScanTableSource, LookupTableSource, etc.)
 * 2. Add connector-specific fields and configuration
 * 3. Implement getScanRuntimeProvider or getLookupRuntimeProvider
 * 4. Consider implementing capabilities like SupportsLimitPushDown, SupportsProjectionPushDown, etc.
 */
public class ConnectorTableSource implements ScanTableSource, SupportsLimitPushDown {

  private final ResolvedSchema schema;
  private final LogicalType[] types;
  // TODO: Add your connector-specific fields
  private final String endpoint;
  private final Integer batchSize;
  private long limit = -1;

  public ConnectorTableSource(ResolvedSchema schema, String endpoint, Integer batchSize) {
    this.schema = schema;
    this.endpoint = endpoint;
    this.batchSize = batchSize;
    this.types =
        schema.getColumns().stream()
            .filter(column -> column.isPhysical())
            .map(Column::getDataType)
            .map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // TODO: Return appropriate changelog mode for your connector
    // For append-only sources, use ChangelogMode.insertOnly()
    // For sources with updates/deletes, configure accordingly
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    // TODO: Implement your data stream provider using Flink 2.x Source API
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment env) {

        // TODO: Create your custom source implementation using the new Source API
        // The Source API (mandatory in Flink 2.x) provides better support for
        // split discovery, watermarks, and event-time processing
        ConnectorSource source =
            new ConnectorSource(endpoint, batchSize, types, schema, limit);

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "{{CONNECTOR_NAME}} Source")
            .returns(
                providerContext
                    .createTypeInformation(schema.toPhysicalRowDataType())
                    .getTypeClass());
      }

      @Override
      public boolean isBounded() {
        // TODO: Return true if your source is bounded (finite), false if unbounded (streaming)
        return limit > 0;
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    ConnectorTableSource copy = new ConnectorTableSource(schema, endpoint, batchSize);
    copy.limit = this.limit;
    return copy;
  }

  @Override
  public String asSummaryString() {
    return "{{CONNECTOR_NAME}}Source";
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit;
  }
}
