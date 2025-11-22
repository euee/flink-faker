package com.example.connector;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * Represents a split of the data source for the {{CONNECTOR_NAME}} connector.
 *
 * <p>A split represents a portion of the source data that can be read independently.
 * Examples: a file, a partition, a shard, a range of IDs, etc.
 *
 * <p>TODO: Define your split structure:
 * 1. Add fields that identify the split (file path, partition ID, offset range, etc.)
 * 2. Implement split() method if your source supports dynamic splitting
 * 3. Make the class serializable for checkpointing
 */
public class ConnectorSourceSplit implements SourceSplit {

  private final String splitId;
  // TODO: Add fields specific to your source
  // Examples:
  // - private final String filePath;
  // - private final int partitionId;
  // - private final long startOffset;
  // - private final long endOffset;
  private final long startRecord;
  private final long endRecord;

  public ConnectorSourceSplit(String splitId, long startRecord, long endRecord) {
    this.splitId = splitId;
    this.startRecord = startRecord;
    this.endRecord = endRecord;
  }

  @Override
  public String splitId() {
    return splitId;
  }

  public long getStartRecord() {
    return startRecord;
  }

  public long getEndRecord() {
    return endRecord;
  }

  public long getRecordCount() {
    return endRecord - startRecord;
  }

  @Override
  public String toString() {
    return "ConnectorSourceSplit{"
        + "splitId='"
        + splitId
        + '\''
        + ", startRecord="
        + startRecord
        + ", endRecord="
        + endRecord
        + '}';
  }
}
