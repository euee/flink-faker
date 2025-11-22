package com.github.euee.flink.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** SourceReader for Cassandra connector. */
public class CassandraSourceReader implements SourceReader<RowData, CassandraSourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraSourceReader.class);

  private final SourceReaderContext context;
  private final CassandraConnectionConfig connectionConfig;
  private final String keyspace;
  private final String table;
  private final String[] fieldNames;
  private final LogicalType[] types;
  private final int fetchSize;

  private final List<CassandraSourceSplit> assignedSplits = new ArrayList<>();
  private CassandraSourceSplit currentSplit;
  private Iterator<Row> currentResultIterator;
  private boolean noMoreSplits = false;

  private CqlSession session;

  public CassandraSourceReader(
      SourceReaderContext context,
      CassandraConnectionConfig connectionConfig,
      String keyspace,
      String table,
      String[] fieldNames,
      LogicalType[] types,
      int fetchSize) {
    this.context = context;
    this.connectionConfig = connectionConfig;
    this.keyspace = keyspace;
    this.table = table;
    this.fieldNames = fieldNames;
    this.types = types;
    this.fetchSize = fetchSize;
  }

  @Override
  public void start() {
    // Initialize Cassandra connection
    LOG.info(
        "Starting Cassandra source reader for {}.{}",
        keyspace,
        table);

    session =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(
                    connectionConfig.getHost(), connectionConfig.getPort()))
            .withLocalDatacenter(connectionConfig.getDatacenter())
            .withAuthCredentials(
                connectionConfig.getUsername(), connectionConfig.getPassword())
            .withKeyspace(keyspace)
            .build();

    LOG.info("Successfully connected to Cassandra");
  }

  @Override
  public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
    // If we have data in current iterator, emit it
    if (currentResultIterator != null && currentResultIterator.hasNext()) {
      Row cassandraRow = currentResultIterator.next();
      RowData flinkRow = CassandraTypeConverter.convertRow(cassandraRow, fieldNames, types);
      output.collect(flinkRow);
      return InputStatus.MORE_AVAILABLE;
    }

    // Current iterator exhausted, get next split
    if (assignedSplits.isEmpty()) {
      return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    currentSplit = assignedSplits.remove(0);
    currentResultIterator = executeSplitQuery(currentSplit);

    // Check if we have data
    if (currentResultIterator.hasNext()) {
      Row cassandraRow = currentResultIterator.next();
      RowData flinkRow = CassandraTypeConverter.convertRow(cassandraRow, fieldNames, types);
      output.collect(flinkRow);
      return InputStatus.MORE_AVAILABLE;
    }

    // No data in this split, try next
    return pollNext(output);
  }

  private Iterator<Row> executeSplitQuery(CassandraSourceSplit split) {
    // Build query for this token range
    // Note: In a production implementation, you'd want to use token() function
    // For simplicity, we'll select all rows (can be optimized with token ranges)

    Select select = QueryBuilder.selectFrom(keyspace, table).all();

    SimpleStatement statement =
        select
            .build()
            .setPageSize(fetchSize);

    // Add token range filtering if not selecting all data
    // WHERE token(partition_key) >= startToken AND token(partition_key) < endToken
    // This requires knowing the partition key columns which could be passed in config

    ResultSet resultSet = session.execute(statement);
    return resultSet.iterator();
  }

  @Override
  public List<CassandraSourceSplit> snapshotState(long checkpointId) {
    // Return unprocessed splits for checkpointing
    List<CassandraSourceSplit> state = new ArrayList<>(assignedSplits);
    if (currentSplit != null) {
      // Add current split back if not fully processed
      state.add(0, currentSplit);
    }
    return state;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    // Data is available if we have splits or current iterator has data
    if ((currentResultIterator != null && currentResultIterator.hasNext())
        || !assignedSplits.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    // For bounded source, return completed future
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void addSplits(List<CassandraSourceSplit> splits) {
    LOG.info("Adding {} splits to reader", splits.size());
    assignedSplits.addAll(splits);
  }

  @Override
  public void notifyNoMoreSplits() {
    LOG.info("No more splits will be assigned");
    noMoreSplits = true;
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing Cassandra source reader");
    if (session != null) {
      session.close();
    }
  }
}
