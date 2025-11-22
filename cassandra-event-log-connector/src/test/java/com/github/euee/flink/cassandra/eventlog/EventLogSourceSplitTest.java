package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link EventLogSourceSplit}. */
class EventLogSourceSplitTest {

  @Test
  void testSplitCreation() {
    EventLogSourceSplit split = new EventLogSourceSplit(5, "last-event-uuid");

    assertThat(split.getShardId()).isEqualTo(5);
    assertThat(split.getLastEventTime()).isEqualTo("last-event-uuid");
    assertThat(split.splitId()).isEqualTo("shard-5");
  }

  @Test
  void testSplitWithNullLastEventTime() {
    EventLogSourceSplit split = new EventLogSourceSplit(0, null);

    assertThat(split.getShardId()).isEqualTo(0);
    assertThat(split.getLastEventTime()).isNull();
    assertThat(split.splitId()).isEqualTo("shard-0");
  }

  @Test
  void testWithLastEventTime() {
    EventLogSourceSplit original = new EventLogSourceSplit(3, "uuid-1");
    EventLogSourceSplit updated = original.withLastEventTime("uuid-2");

    assertThat(updated.getShardId()).isEqualTo(3);
    assertThat(updated.getLastEventTime()).isEqualTo("uuid-2");

    // Original should not be modified
    assertThat(original.getLastEventTime()).isEqualTo("uuid-1");
  }

  @Test
  void testEquals() {
    EventLogSourceSplit split1 = new EventLogSourceSplit(1, "uuid-a");
    EventLogSourceSplit split2 = new EventLogSourceSplit(1, "uuid-a");
    EventLogSourceSplit split3 = new EventLogSourceSplit(1, "uuid-b");
    EventLogSourceSplit split4 = new EventLogSourceSplit(2, "uuid-a");

    assertThat(split1).isEqualTo(split2);
    assertThat(split1).isNotEqualTo(split3);
    assertThat(split1).isNotEqualTo(split4);
  }

  @Test
  void testHashCode() {
    EventLogSourceSplit split1 = new EventLogSourceSplit(1, "uuid-a");
    EventLogSourceSplit split2 = new EventLogSourceSplit(1, "uuid-a");

    assertThat(split1.hashCode()).isEqualTo(split2.hashCode());
  }

  @Test
  void testToString() {
    EventLogSourceSplit split = new EventLogSourceSplit(7, "test-uuid");

    String str = split.toString();
    assertThat(str).contains("shardId=7");
    assertThat(str).contains("lastEventTime='test-uuid'");
  }
}
