package com.github.euee.flink.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/** SplitEnumerator for Cassandra connector. */
public class CassandraSplitEnumerator
    implements SplitEnumerator<CassandraSourceSplit, CassandraEnumeratorState> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraSplitEnumerator.class);

  private final SplitEnumeratorContext<CassandraSourceSplit> context;
  private final List<CassandraSourceSplit> pendingSplits;

  /**
   * Constructor for initial execution (no checkpoint).
   */
  public CassandraSplitEnumerator(
      SplitEnumeratorContext<CassandraSourceSplit> context,
      CassandraConnectionConfig connectionConfig,
      String keyspace,
      String table,
      int parallelism) {
    this.context = context;
    this.pendingSplits = new ArrayList<>();

    // Discover splits
    // For simplicity, create one split per desired parallelism
    // In production, you'd want to query Cassandra for actual token ranges
    discoverSplits(parallelism);
  }

  /**
   * Constructor for restoring from checkpoint.
   */
  public CassandraSplitEnumerator(
      SplitEnumeratorContext<CassandraSourceSplit> context, CassandraEnumeratorState state) {
    this.context = context;
    this.pendingSplits = new ArrayList<>(state.getPendingSplits());
  }

  private void discoverSplits(int numSplits) {
    // Create splits based on token ranges
    // Cassandra uses token values from Long.MIN_VALUE to Long.MAX_VALUE
    // We'll create equal-sized token ranges

    long tokenRange = (Long.MAX_VALUE / numSplits) * 2; // Account for full range
    long currentToken = Long.MIN_VALUE;

    for (int i = 0; i < numSplits; i++) {
      long startToken = currentToken;
      long endToken;

      if (i == numSplits - 1) {
        // Last split goes to MAX_VALUE
        endToken = Long.MAX_VALUE;
      } else {
        endToken = startToken + tokenRange;
      }

      CassandraSourceSplit split =
          new CassandraSourceSplit("split-" + i, startToken, endToken);
      pendingSplits.add(split);

      currentToken = endToken;
    }

    LOG.info("Discovered {} splits for Cassandra table", numSplits);
  }

  @Override
  public void start() {
    LOG.info("Starting Cassandra split enumerator");
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    if (!pendingSplits.isEmpty()) {
      CassandraSourceSplit split = pendingSplits.remove(0);
      context.assignSplit(split, subtaskId);
      LOG.info("Assigned split {} to subtask {}", split.splitId(), subtaskId);
    } else {
      context.signalNoMoreSplits(subtaskId);
      LOG.info("No more splits available for subtask {}", subtaskId);
    }
  }

  @Override
  public void addSplitsBack(List<CassandraSourceSplit> splits, int subtaskId) {
    LOG.info("Adding {} splits back from failed subtask {}", splits.size(), subtaskId);
    pendingSplits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    // New reader registered, no immediate action needed
  }

  @Override
  public CassandraEnumeratorState snapshotState(long checkpointId) throws Exception {
    return new CassandraEnumeratorState(pendingSplits);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing Cassandra split enumerator");
  }
}
