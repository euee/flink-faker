package com.example.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Source function that generates/reads data for the {{CONNECTOR_NAME}} connector.
 *
 * TODO: Customize this source function for your connector:
 * 1. Initialize your data source/client in the open() method
 * 2. Implement the run() method to read/generate data
 * 3. Implement proper type conversion in convertToRowData()
 * 4. Handle cleanup in cancel() and close() methods
 */
public class ConnectorSourceFunction extends RichSourceFunction<RowData> {

  private final String endpoint;
  private final Integer batchSize;
  private final LogicalType[] types;
  private final ResolvedSchema schema;
  private volatile boolean running = true;
  private long limit = -1;
  private long emittedRecords = 0;

  // TODO: Add your connector-specific client/connection fields
  // Example: private YourClient client;

  public ConnectorSourceFunction(
      String endpoint, Integer batchSize, LogicalType[] types, ResolvedSchema schema) {
    this.endpoint = endpoint;
    this.batchSize = batchSize;
    this.types = types;
    this.schema = schema;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // TODO: Initialize your connector client/connection
    // Example:
    // this.client = new YourClient(endpoint);
    // this.client.connect();
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    // TODO: Implement your data reading/generation logic
    // This is where you fetch data from your source and emit it

    while (running && (limit < 0 || emittedRecords < limit)) {
      // TODO: Replace this example with actual data fetching logic
      // Example: fetch a batch of records from your source
      // List<Record> records = client.fetchBatch(batchSize);

      // For demonstration, we'll create a sample row
      // In practice, you would read from your actual data source
      RowData row = createSampleRow();

      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(row);
        emittedRecords++;
      }

      // TODO: Implement appropriate backpressure/rate limiting if needed
      // Example: Thread.sleep(100);
    }
  }

  /**
   * Create a sample row. TODO: Replace this with actual data conversion from your source.
   */
  private RowData createSampleRow() {
    GenericRowData row = new GenericRowData(types.length);

    for (int i = 0; i < types.length; i++) {
      // TODO: Implement proper type conversion based on your source data
      // This is just an example
      Object value = convertToFlinkType("sample_value", types[i]);
      row.setField(i, value);
    }

    return row;
  }

  /**
   * Convert source data to Flink internal types.
   *
   * TODO: Implement conversion logic for all data types your connector supports
   */
  private Object convertToFlinkType(Object sourceValue, LogicalType logicalType) {
    if (sourceValue == null) {
      return null;
    }

    // TODO: Add proper type conversion logic
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
      // TODO: Add more type conversions as needed (DECIMAL, DATE, TIMESTAMP, ARRAY, MAP, ROW, etc.)
      default:
        throw new UnsupportedOperationException(
            "Type " + logicalType.getTypeRoot() + " is not supported yet");
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public void close() throws Exception {
    super.close();

    // TODO: Clean up resources (close connections, clients, etc.)
    // Example:
    // if (client != null) {
    //   client.close();
    // }
  }
}
