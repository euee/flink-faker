package com.github.euee.flink.cassandra;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

/** Source implementation for Cassandra connector using Flink 2.x Source API. */
public class CassandraSource
    implements Source<RowData, CassandraSourceSplit, CassandraEnumeratorState> {

  private final CassandraConnectionConfig connectionConfig;
  private final String keyspace;
  private final String table;
  private final String[] fieldNames;
  private final LogicalType[] types;
  private final int fetchSize;
  private final int parallelism;

  public CassandraSource(
      CassandraConnectionConfig connectionConfig,
      String keyspace,
      String table,
      String[] fieldNames,
      LogicalType[] types,
      int fetchSize,
      int parallelism) {
    this.connectionConfig = connectionConfig;
    this.keyspace = keyspace;
    this.table = table;
    this.fieldNames = fieldNames;
    this.types = types;
    this.fetchSize = fetchSize;
    this.parallelism = parallelism;
  }

  @Override
  public Boundedness getBoundedness() {
    // Cassandra table source is bounded (reads existing data)
    return Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<RowData, CassandraSourceSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return new CassandraSourceReader(
        readerContext, connectionConfig, keyspace, table, fieldNames, types, fetchSize);
  }

  @Override
  public SplitEnumerator<CassandraSourceSplit, CassandraEnumeratorState> createEnumerator(
      SplitEnumeratorContext<CassandraSourceSplit> enumContext) throws Exception {
    return new CassandraSplitEnumerator(enumContext, connectionConfig, keyspace, table, parallelism);
  }

  @Override
  public SplitEnumerator<CassandraSourceSplit, CassandraEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<CassandraSourceSplit> enumContext,
      CassandraEnumeratorState checkpoint)
      throws Exception {
    return new CassandraSplitEnumerator(enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<CassandraSourceSplit> getSplitSerializer() {
    return new CassandraSourceSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<CassandraEnumeratorState> getEnumeratorCheckpointSerializer() {
    return new CassandraEnumeratorStateSerializer();
  }
}
