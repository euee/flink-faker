package com.example.connector;

import java.util.ArrayList;
import java.util.List;

/**
 * State for ConnectorSplitEnumerator used in checkpointing.
 *
 * <p>This state allows the enumerator to resume split assignment after a failure.
 *
 * <p>TODO: Add fields needed to restore the enumerator state:
 * 1. Pending splits that haven't been assigned yet
 * 2. Discovery state (e.g., last scanned file/partition)
 * 3. Any other enumerator-specific state
 */
public class ConnectorEnumeratorState {

  private final List<ConnectorSourceSplit> pendingSplits;
  private final long nextSplitId;

  public ConnectorEnumeratorState(List<ConnectorSourceSplit> pendingSplits, long nextSplitId) {
    this.pendingSplits = new ArrayList<>(pendingSplits);
    this.nextSplitId = nextSplitId;
  }

  public List<ConnectorSourceSplit> getPendingSplits() {
    return new ArrayList<>(pendingSplits);
  }

  public long getNextSplitId() {
    return nextSplitId;
  }
}
