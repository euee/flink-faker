package com.example.connector;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * SourceReader for {{CONNECTOR_NAME}} connector.
 *
 * <p>The SourceReader is responsible for reading data from assigned splits and converting
 * it to RowData format.
 *
 * <p>TODO: Implement your data reading logic:
 * 1. Initialize connection/client in constructor or when splits are assigned
 * 2. Read data from assigned splits in pollNext()
 * 3. Convert source data to RowData format
 * 4. Handle split completion and reader lifecycle
 * 5. Implement checkpointing via snapshotState() if needed
 */
public class ConnectorSourceReader implements SourceReader<RowData, ConnectorSourceSplit> {

  private final SourceReaderContext context;
  private final String endpoint;
  private final Integer batchSize;
  private final LogicalType[] types;
  private final ResolvedSchema schema;
  private final long totalLimit;

  private final List<ConnectorSourceSplit> assignedSplits = new ArrayList<>();
  private ConnectorSourceSplit currentSplit;
  private long currentPosition = 0;
  private long totalRecordsRead = 0;
  private boolean noMoreSplits = false;

  // TODO: Add your connector client/connection
  // private YourClient client;

  public ConnectorSourceReader(
      SourceReaderContext context,
      String endpoint,
      Integer batchSize,
      LogicalType[] types,
      ResolvedSchema schema,
      long totalLimit) {
    this.context = context;
    this.endpoint = endpoint;
    this.batchSize = batchSize;
    this.types = types;
    this.schema = schema;
    this.totalLimit = totalLimit;

    // TODO: Initialize your connector client
    // this.client = new YourClient(endpoint);
  }

  @Override
  public void start() {
    // TODO: Perform any initialization needed before reading
    // Examples: open connection, authenticate, etc.
  }

  @Override
  public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
    // Check if we've reached the limit
    if (totalLimit > 0 && totalRecordsRead >= totalLimit) {
      return InputStatus.END_OF_INPUT;
    }

    // Get next split if we don't have one or current is exhausted
    if (currentSplit == null || currentPosition >= currentSplit.getEndRecord()) {
      if (assignedSplits.isEmpty()) {
        return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
      }
      currentSplit = assignedSplits.remove(0);
      currentPosition = currentSplit.getStartRecord();
    }

    // TODO: Read actual data from your source
    // Example: Record record = client.readRecord(currentSplit, currentPosition);

    // Create sample row (replace with actual data reading)
    RowData row = createSampleRow();
    output.collect(row);

    currentPosition++;
    totalRecordsRead++;

    return InputStatus.MORE_AVAILABLE;
  }

  /**
   * Create a sample row.
   *
   * TODO: Replace with actual data conversion from your source
   */
  private RowData createSampleRow() {
    GenericRowData row = new GenericRowData(types.length);

    for (int i = 0; i < types.length; i++) {
      // TODO: Convert your source data to appropriate Flink types
      Object value = convertToFlinkType("sample_value_" + currentPosition, types[i]);
      row.setField(i, value);
    }

    return row;
  }

  /**
   * Convert source data to Flink internal types.
   *
   * TODO: Implement proper type conversion for your data format
   */
  private Object convertToFlinkType(Object sourceValue, LogicalType logicalType) {
    if (sourceValue == null) {
      return null;
    }

    switch (logicalType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return StringData.fromString(sourceValue.toString());
      case BOOLEAN:
        return Boolean.parseBoolean(sourceValue.toString());
      case TINYINT:
        return Byte.parseByte(sourceValue.toString());
      case SMALLINT:
        return Short.parseShort(sourceValue.toString());
      case INTEGER:
        return Integer.parseInt(sourceValue.toString());
      case BIGINT:
        return Long.parseLong(sourceValue.toString());
      case FLOAT:
        return Float.parseFloat(sourceValue.toString());
      case DOUBLE:
        return Double.parseDouble(sourceValue.toString());
      // TODO: Add more type conversions (DECIMAL, TIMESTAMP, ARRAY, MAP, ROW, etc.)
      default:
        throw new UnsupportedOperationException(
            "Type " + logicalType.getTypeRoot() + " is not supported yet");
    }
  }

  @Override
  public List<ConnectorSourceSplit> snapshotState(long checkpointId) {
    // TODO: Return current state for checkpointing
    // Include the current split and position so reading can resume after failure
    List<ConnectorSourceSplit> state = new ArrayList<>(assignedSplits);
    if (currentSplit != null && currentPosition < currentSplit.getEndRecord()) {
      // Create a new split representing remaining data in current split
      state.add(
          new ConnectorSourceSplit(
              currentSplit.splitId(), currentPosition, currentSplit.getEndRecord()));
    }
    return state;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    // TODO: Return a future that completes when data is available to read
    // For pull-based sources, this can return a completed future
    // For push-based sources, complete when data arrives
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void addSplits(List<ConnectorSourceSplit> splits) {
    // TODO: Handle newly assigned splits
    assignedSplits.addAll(splits);
  }

  @Override
  public void notifyNoMoreSplits() {
    // Called when all splits have been assigned
    noMoreSplits = true;
  }

  @Override
  public void close() throws Exception {
    // TODO: Clean up resources (close connections, clients, etc.)
    // if (client != null) {
    //   client.close();
    // }
  }
}
