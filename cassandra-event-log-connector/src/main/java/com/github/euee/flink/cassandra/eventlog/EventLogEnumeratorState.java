package com.github.euee.flink.cassandra.eventlog;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** State for the EventLogSplitEnumerator to support checkpointing. */
public class EventLogEnumeratorState implements Serializable {

  private static final long serialVersionUID = 1L;

  private final List<EventLogSourceSplit> remainingSplits;

  public EventLogEnumeratorState(List<EventLogSourceSplit> remainingSplits) {
    this.remainingSplits = remainingSplits;
  }

  public List<EventLogSourceSplit> getRemainingSplits() {
    return remainingSplits;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventLogEnumeratorState that = (EventLogEnumeratorState) o;
    return Objects.equals(remainingSplits, that.remainingSplits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(remainingSplits);
  }

  @Override
  public String toString() {
    return "EventLogEnumeratorState{" + "remainingSplits=" + remainingSplits + '}';
  }
}
