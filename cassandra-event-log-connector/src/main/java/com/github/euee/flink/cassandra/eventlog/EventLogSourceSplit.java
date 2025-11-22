package com.github.euee.flink.cassandra.eventlog;

import org.apache.flink.api.connector.source.SourceSplit;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a split for reading from a specific shard in the event log table.
 *
 * <p>Each split corresponds to one shard and tracks the last processed event time (timeuuid) for
 * that shard to support incremental polling.
 */
public class EventLogSourceSplit implements SourceSplit {

  private final int shardId;
  private final String lastEventTime; // TimeUUID as string, nullable for initial state

  public EventLogSourceSplit(int shardId, @Nullable String lastEventTime) {
    this.shardId = shardId;
    this.lastEventTime = lastEventTime;
  }

  public int getShardId() {
    return shardId;
  }

  @Nullable
  public String getLastEventTime() {
    return lastEventTime;
  }

  /**
   * Creates a new split with updated last event time.
   *
   * @param newLastEventTime The new last event time
   * @return A new split with updated state
   */
  public EventLogSourceSplit withLastEventTime(String newLastEventTime) {
    return new EventLogSourceSplit(this.shardId, newLastEventTime);
  }

  @Override
  public String splitId() {
    return "shard-" + shardId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventLogSourceSplit that = (EventLogSourceSplit) o;
    return shardId == that.shardId && Objects.equals(lastEventTime, that.lastEventTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, lastEventTime);
  }

  @Override
  public String toString() {
    return "EventLogSourceSplit{"
        + "shardId="
        + shardId
        + ", lastEventTime='"
        + lastEventTime
        + '\''
        + '}';
  }
}
