package com.example.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Source implementation for {{CONNECTOR_NAME}} connector using Flink 2.x Source API.
 *
 * <p>The Source API (introduced in Flink 1.12, mandatory in Flink 2.0+) provides better
 * support for features like split discovery, watermarks, and event-time processing.
 *
 * <p>TODO: Customize this source for your connector:
 * 1. Implement createReader() to return your custom SourceReader
 * 2. Implement createEnumerator() to manage source splits
 * 3. Define how your data source is split (files, partitions, shards, etc.)
 * 4. Handle checkpointing and state management
 */
public class ConnectorSource
    implements Source<RowData, ConnectorSourceSplit, ConnectorEnumeratorState> {

  private final String endpoint;
  private final Integer batchSize;
  private final LogicalType[] types;
  private final ResolvedSchema schema;
  private final long limit;

  public ConnectorSource(
      String endpoint,
      Integer batchSize,
      LogicalType[] types,
      ResolvedSchema schema,
      long limit) {
    this.endpoint = endpoint;
    this.batchSize = batchSize;
    this.types = types;
    this.schema = schema;
    this.limit = limit;
  }

  @Override
  public Boundedness getBoundedness() {
    // TODO: Return BOUNDED if your source has finite data, CONTINUOUS_UNBOUNDED for streaming
    return limit > 0 ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<RowData, ConnectorSourceSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    // TODO: Create and return your custom SourceReader
    return new ConnectorSourceReader(readerContext, endpoint, batchSize, types, schema, limit);
  }

  @Override
  public SplitEnumerator<ConnectorSourceSplit, ConnectorEnumeratorState> createEnumerator(
      SplitEnumeratorContext<ConnectorSourceSplit> enumContext) throws Exception {
    // TODO: Create the split enumerator for initial execution
    // The enumerator is responsible for discovering and assigning splits to readers
    return new ConnectorSplitEnumerator(enumContext, limit);
  }

  @Override
  public SplitEnumerator<ConnectorSourceSplit, ConnectorEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<ConnectorSourceSplit> enumContext,
      ConnectorEnumeratorState checkpoint)
      throws Exception {
    // TODO: Restore the split enumerator from checkpoint state
    return new ConnectorSplitEnumerator(enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<ConnectorSourceSplit> getSplitSerializer() {
    // TODO: Return serializer for your split type
    return new ConnectorSourceSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<ConnectorEnumeratorState> getEnumeratorCheckpointSerializer() {
    // TODO: Return serializer for enumerator checkpoint state
    return new ConnectorEnumeratorStateSerializer();
  }
}
