package com.github.euee.flink.cassandra.eventlog;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Split enumerator for event log source.
 *
 * <p>This enumerator creates one split per shard and assigns them to readers. For unbounded
 * sources, splits are assigned once at startup.
 */
public class EventLogSplitEnumerator
    implements SplitEnumerator<EventLogSourceSplit, EventLogEnumeratorState> {

  private static final Logger LOG = LoggerFactory.getLogger(EventLogSplitEnumerator.class);

  private final SplitEnumeratorContext<EventLogSourceSplit> context;
  private final int numShards;
  private final List<EventLogSourceSplit> pendingSplits;

  public EventLogSplitEnumerator(
      SplitEnumeratorContext<EventLogSourceSplit> context,
      int numShards,
      @Nullable EventLogEnumeratorState restoredState) {
    this.context = context;
    this.numShards = numShards;

    if (restoredState != null) {
      this.pendingSplits = new ArrayList<>(restoredState.getRemainingSplits());
      LOG.info("Restored {} pending splits from checkpoint", pendingSplits.size());
    } else {
      // Create initial splits (one per shard)
      this.pendingSplits = new ArrayList<>(numShards);
      for (int i = 0; i < numShards; i++) {
        pendingSplits.add(new EventLogSourceSplit(i, null)); // null = no last event time yet
      }
      LOG.info("Created {} initial splits for shards 0-{}", numShards, numShards - 1);
    }
  }

  @Override
  public void start() {
    // No periodic split discovery needed for event log
    // All splits are created upfront and assigned once
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // This is for dynamic split assignment - not used in our case
    // We assign splits immediately when readers register
  }

  @Override
  public void addSplitsBack(List<EventLogSourceSplit> splits, int subtaskId) {
    // Add splits back to pending list (e.g., when a reader fails)
    LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
    pendingSplits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    // Assign splits when a new reader registers
    assignSplits();
  }

  private void assignSplits() {
    if (pendingSplits.isEmpty()) {
      LOG.debug("No pending splits to assign");
      return;
    }

    int numReaders = context.currentParallelism();
    if (numReaders == 0) {
      LOG.warn("No readers available to assign splits");
      return;
    }

    // Distribute splits evenly across readers
    // Each reader gets approximately numShards / numReaders splits
    for (int readerId = 0; readerId < numReaders && !pendingSplits.isEmpty(); readerId++) {
      List<EventLogSourceSplit> splitsForReader = new ArrayList<>();

      // Assign splits in round-robin fashion
      int splitsPerReader = (int) Math.ceil((double) pendingSplits.size() / (numReaders - readerId));
      for (int i = 0; i < splitsPerReader && !pendingSplits.isEmpty(); i++) {
        splitsForReader.add(pendingSplits.remove(0));
      }

      if (!splitsForReader.isEmpty()) {
        LOG.info("Assigning {} splits to reader {}", splitsForReader.size(), readerId);
        java.util.Map<Integer, List<EventLogSourceSplit>> assignment =
            java.util.Collections.singletonMap(readerId, splitsForReader);
        context.assignSplits(
            new org.apache.flink.api.connector.source.SplitsAssignment<>(assignment));
      }
    }

    // Signal no more splits for unbounded source
    // (readers will continue polling their assigned shards indefinitely)
    for (int readerId : context.registeredReaders().keySet()) {
      context.signalNoMoreSplits(readerId);
    }
  }

  @Override
  public EventLogEnumeratorState snapshotState(long checkpointId) throws Exception {
    LOG.debug("Snapshotting enumerator state for checkpoint {}", checkpointId);
    return new EventLogEnumeratorState(new ArrayList<>(pendingSplits));
  }

  @Override
  public void close() throws IOException {
    // Nothing to close
  }
}
