package com.example.connector;

import java.io.IOException;
import java.util.*;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Lookup function for {{CONNECTOR_NAME}} connector.
 *
 * TODO: Implement this if your connector supports lookup joins (dimension table lookups)
 * This is optional - only needed if you want to support temporal table joins
 * If not needed, you can remove this class and remove LookupTableSource from ConnectorTableSource
 */
public class ConnectorLookupFunction extends LookupFunction {

  private final String endpoint;
  private final LogicalType[] types;
  private final List<Integer> keyIndices;

  // TODO: Add your connector client for lookups
  // private YourClient client;

  public ConnectorLookupFunction(
      String endpoint, LogicalType[] types, int[][] keys) {
    this.endpoint = endpoint;
    this.types = types;

    keyIndices = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      // we don't support nested rows for now, so this is one-dimensional
      keyIndices.add(keys[i][0]);
    }
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);

    // TODO: Initialize your lookup client/connection
    // Example:
    // this.client = new YourClient(endpoint);
    // this.client.connect();
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    // TODO: Implement lookup logic
    // 1. Extract key values from keyRow
    // 2. Query your data source using the key
    // 3. Convert results to RowData and return

    // Example structure:
    GenericRowData resultRow = new GenericRowData(types.length);

    // Extract keys
    Object[] keyValues = new Object[keyIndices.size()];
    for (int i = 0; i < keyIndices.size(); i++) {
      keyValues[i] = ((GenericRowData) keyRow).getField(i);
    }

    // TODO: Perform lookup query
    // Example:
    // Record record = client.lookup(keyValues);
    // if (record == null) {
    //   return Collections.emptyList();
    // }

    // TODO: Convert result to RowData
    // Set key fields first
    int keyCount = 0;
    for (int i = 0; i < types.length; i++) {
      if (keyIndices.contains(i)) {
        resultRow.setField(i, keyValues[keyCount]);
        keyCount++;
      } else {
        // TODO: Set non-key fields from lookup result
        // resultRow.setField(i, convertValue(record.getField(i), types[i]));
      }
    }

    return Collections.singleton(resultRow);
  }

  @Override
  public Set<FunctionRequirement> getRequirements() {
    return Collections.emptySet();
  }

  @Override
  public boolean isDeterministic() {
    // TODO: Return true if lookups are deterministic (same input always gives same output)
    // Return false if data can change (typical for dimension tables)
    return false;
  }

  @Override
  public void close() throws Exception {
    super.close();

    // TODO: Clean up resources
    // Example:
    // if (client != null) {
    //   client.close();
    // }
  }
}
