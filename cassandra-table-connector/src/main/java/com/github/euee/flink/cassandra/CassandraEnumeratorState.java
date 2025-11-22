package com.github.euee.flink.cassandra;

import java.util.ArrayList;
import java.util.List;

/** State for CassandraSplitEnumerator used in checkpointing. */
public class CassandraEnumeratorState {

  private final List<CassandraSourceSplit> pendingSplits;

  public CassandraEnumeratorState(List<CassandraSourceSplit> pendingSplits) {
    this.pendingSplits = new ArrayList<>(pendingSplits);
  }

  public List<CassandraSourceSplit> getPendingSplits() {
    return new ArrayList<>(pendingSplits);
  }
}
