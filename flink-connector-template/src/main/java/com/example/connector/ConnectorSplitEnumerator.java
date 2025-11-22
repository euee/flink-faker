package com.example.connector;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * SplitEnumerator for {{CONNECTOR_NAME}} connector.
 *
 * <p>The SplitEnumerator is responsible for:
 * 1. Discovering available splits (e.g., files, partitions, shards)
 * 2. Assigning splits to SourceReaders
 * 3. Handling split discovery for dynamic sources
 *
 * <p>TODO: Implement split discovery and assignment logic:
 * 1. Discover splits from your source (e.g., list files, query partitions)
 * 2. Assign splits to available readers (consider data locality if applicable)
 * 3. Handle dynamic split discovery if your source changes over time
 * 4. Checkpoint enumerator state for fault tolerance
 */
public class ConnectorSplitEnumerator
    implements SplitEnumerator<ConnectorSourceSplit, ConnectorEnumeratorState> {

  private final SplitEnumeratorContext<ConnectorSourceSplit> context;
  private final List<ConnectorSourceSplit> pendingSplits;
  private long nextSplitId;

  /**
   * Constructor for initial execution (no checkpoint).
   */
  public ConnectorSplitEnumerator(
      SplitEnumeratorContext<ConnectorSourceSplit> context, long totalLimit) {
    this.context = context;
    this.pendingSplits = new ArrayList<>();
    this.nextSplitId = 0;

    // TODO: Discover initial splits from your source
    // Examples:
    // - List files in a directory
    // - Query available partitions
    // - Get shard information
    discoverSplits(totalLimit);
  }

  /**
   * Constructor for restoring from checkpoint.
   */
  public ConnectorSplitEnumerator(
      SplitEnumeratorContext<ConnectorSourceSplit> context, ConnectorEnumeratorState state) {
    this.context = context;
    this.pendingSplits = new ArrayList<>(state.getPendingSplits());
    this.nextSplitId = state.getNextSplitId();
  }

  /**
   * Discover splits from the data source.
   *
   * TODO: Implement your split discovery logic
   */
  private void discoverSplits(long totalLimit) {
    // TODO: Replace with actual split discovery
    // For example, this creates simple splits based on record ranges
    int numSplits = context.currentParallelism();
    if (totalLimit > 0) {
      long recordsPerSplit = Math.max(1, totalLimit / numSplits);
      for (int i = 0; i < numSplits; i++) {
        long start = i * recordsPerSplit;
        long end = (i == numSplits - 1) ? totalLimit : (i + 1) * recordsPerSplit;
        if (start < end) {
          pendingSplits.add(new ConnectorSourceSplit("split-" + nextSplitId++, start, end));
        }
      }
    } else {
      // For unbounded sources, create one split per reader
      for (int i = 0; i < numSplits; i++) {
        pendingSplits.add(
            new ConnectorSourceSplit("split-" + nextSplitId++, 0, Long.MAX_VALUE));
      }
    }
  }

  @Override
  public void start() {
    // TODO: Perform any initialization needed
    // Examples: open connection, start split discovery thread, etc.
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // Called when a reader requests a split
    // Assign a pending split if available
    if (!pendingSplits.isEmpty()) {
      ConnectorSourceSplit split = pendingSplits.remove(0);
      context.assignSplit(split, subtaskId);
    } else {
      // No more splits available
      context.signalNoMoreSplits(subtaskId);
    }
  }

  @Override
  public void addSplitsBack(List<ConnectorSourceSplit> splits, int subtaskId) {
    // Called when a reader fails and returns its splits
    // Add them back to pending splits for reassignment
    pendingSplits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    // Called when a new reader is registered
    // Optionally assign splits immediately
    // For on-demand assignment, this can be empty (reader will call handleSplitRequest)
  }

  @Override
  public ConnectorEnumeratorState snapshotState(long checkpointId) throws Exception {
    // TODO: Create checkpoint state
    return new ConnectorEnumeratorState(pendingSplits, nextSplitId);
  }

  @Override
  public void close() throws IOException {
    // TODO: Clean up resources
    // Examples: close connections, stop discovery threads, etc.
  }
}
