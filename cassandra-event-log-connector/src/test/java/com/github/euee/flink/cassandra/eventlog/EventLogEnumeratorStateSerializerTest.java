package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link EventLogEnumeratorStateSerializer}. */
class EventLogEnumeratorStateSerializerTest {

  private EventLogEnumeratorStateSerializer serializer;

  @BeforeEach
  void setUp() {
    serializer = new EventLogEnumeratorStateSerializer();
  }

  @Test
  void testVersion() {
    assertThat(serializer.getVersion()).isEqualTo(1);
  }

  @Test
  void testSerializeDeserializeWithMultipleSplits() throws Exception {
    List<EventLogSourceSplit> splits =
        Arrays.asList(
            new EventLogSourceSplit(0, "uuid-0"),
            new EventLogSourceSplit(1, "uuid-1"),
            new EventLogSourceSplit(2, null),
            new EventLogSourceSplit(15, "uuid-15"));

    EventLogEnumeratorState original = new EventLogEnumeratorState(splits);

    byte[] serialized = serializer.serialize(original);
    EventLogEnumeratorState deserialized =
        serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.getRemainingSplits()).hasSize(4);
    assertThat(deserialized.getRemainingSplits().get(0).getShardId()).isEqualTo(0);
    assertThat(deserialized.getRemainingSplits().get(1).getShardId()).isEqualTo(1);
    assertThat(deserialized.getRemainingSplits().get(2).getShardId()).isEqualTo(2);
    assertThat(deserialized.getRemainingSplits().get(2).getLastEventTime()).isNull();
    assertThat(deserialized.getRemainingSplits().get(3).getShardId()).isEqualTo(15);
  }

  @Test
  void testSerializeDeserializeWithEmptySplits() throws Exception {
    EventLogEnumeratorState original = new EventLogEnumeratorState(Collections.emptyList());

    byte[] serialized = serializer.serialize(original);
    EventLogEnumeratorState deserialized =
        serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.getRemainingSplits()).isEmpty();
  }

  @Test
  void testSerializeDeserializeWithSingleSplit() throws Exception {
    List<EventLogSourceSplit> splits =
        Collections.singletonList(new EventLogSourceSplit(7, "test-uuid"));

    EventLogEnumeratorState original = new EventLogEnumeratorState(splits);

    byte[] serialized = serializer.serialize(original);
    EventLogEnumeratorState deserialized =
        serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.getRemainingSplits()).hasSize(1);
    assertThat(deserialized.getRemainingSplits().get(0).getShardId()).isEqualTo(7);
    assertThat(deserialized.getRemainingSplits().get(0).getLastEventTime()).isEqualTo("test-uuid");
  }

  @Test
  void testDeserializeWithWrongVersion() {
    EventLogEnumeratorState state =
        new EventLogEnumeratorState(Collections.singletonList(new EventLogSourceSplit(1, "test")));

    assertThatThrownBy(
            () -> {
              byte[] serialized = serializer.serialize(state);
              serializer.deserialize(999, serialized); // Wrong version
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Unsupported version");
  }

  @Test
  void testSerializeDeserializeAllShards() throws Exception {
    // Test with all 16 shards
    List<EventLogSourceSplit> splits =
        Arrays.asList(
            new EventLogSourceSplit(0, null),
            new EventLogSourceSplit(1, null),
            new EventLogSourceSplit(2, null),
            new EventLogSourceSplit(3, null),
            new EventLogSourceSplit(4, null),
            new EventLogSourceSplit(5, null),
            new EventLogSourceSplit(6, null),
            new EventLogSourceSplit(7, null),
            new EventLogSourceSplit(8, null),
            new EventLogSourceSplit(9, null),
            new EventLogSourceSplit(10, null),
            new EventLogSourceSplit(11, null),
            new EventLogSourceSplit(12, null),
            new EventLogSourceSplit(13, null),
            new EventLogSourceSplit(14, null),
            new EventLogSourceSplit(15, null));

    EventLogEnumeratorState original = new EventLogEnumeratorState(splits);

    byte[] serialized = serializer.serialize(original);
    EventLogEnumeratorState deserialized =
        serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.getRemainingSplits()).hasSize(16);

    // Verify all shards are present
    for (int i = 0; i < 16; i++) {
      assertThat(deserialized.getRemainingSplits().get(i).getShardId()).isEqualTo(i);
    }
  }
}
