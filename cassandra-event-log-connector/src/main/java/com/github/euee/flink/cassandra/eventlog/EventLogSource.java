package com.github.euee.flink.cassandra.eventlog;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Unbounded Flink Source for reading from Cassandra event log tables.
 *
 * <p>This source implements a two-phase reading strategy: 1. **Initial bulk read**: Reads all
 * existing events from all shards 2. **Continuous polling**: Polls for new events at regular
 * intervals
 *
 * <p>Each shard is assigned to a reader which maintains the last processed event_time (timeuuid)
 * and queries only for newer events.
 */
public class EventLogSource
    implements Source<RowData, EventLogSourceSplit, EventLogEnumeratorState> {

  private static final long serialVersionUID = 1L;

  private final EventLogConnectionConfig config;
  private final String[] fieldNames;
  private final LogicalType[] fieldTypes;

  public EventLogSource(
      EventLogConnectionConfig config, String[] fieldNames, LogicalType[] fieldTypes) {
    this.config = config;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public Boundedness getBoundedness() {
    // This is an unbounded streaming source
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<RowData, EventLogSourceSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return new EventLogSourceReader(config, fieldNames, fieldTypes, readerContext);
  }

  @Override
  public SplitEnumerator<EventLogSourceSplit, EventLogEnumeratorState> createEnumerator(
      SplitEnumeratorContext<EventLogSourceSplit> enumContext) throws Exception {
    return new EventLogSplitEnumerator(enumContext, config.getNumShards(), null);
  }

  @Override
  public SplitEnumerator<EventLogSourceSplit, EventLogEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<EventLogSourceSplit> enumContext, EventLogEnumeratorState checkpoint)
      throws Exception {
    return new EventLogSplitEnumerator(enumContext, config.getNumShards(), checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<EventLogSourceSplit> getSplitSerializer() {
    return new EventLogSourceSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<EventLogEnumeratorState> getEnumeratorCheckpointSerializer() {
    return new EventLogEnumeratorStateSerializer();
  }

  public TypeInformation<RowData> getProducedType() {
    return TypeInformation.of(RowData.class);
  }
}
