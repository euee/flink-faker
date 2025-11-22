package com.github.euee.flink.cassandra.eventlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link EventLogSourceSplitSerializer}. */
class EventLogSourceSplitSerializerTest {

  private EventLogSourceSplitSerializer serializer;

  @BeforeEach
  void setUp() {
    serializer = new EventLogSourceSplitSerializer();
  }

  @Test
  void testVersion() {
    assertThat(serializer.getVersion()).isEqualTo(1);
  }

  @Test
  void testSerializeDeserializeWithLastEventTime() throws Exception {
    EventLogSourceSplit original = new EventLogSourceSplit(5, "550e8400-e29b-41d4-a716-446655440000");

    byte[] serialized = serializer.serialize(original);
    EventLogSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.getShardId()).isEqualTo(5);
    assertThat(deserialized.getLastEventTime())
        .isEqualTo("550e8400-e29b-41d4-a716-446655440000");
  }

  @Test
  void testSerializeDeserializeWithNullLastEventTime() throws Exception {
    EventLogSourceSplit original = new EventLogSourceSplit(10, null);

    byte[] serialized = serializer.serialize(original);
    EventLogSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.getShardId()).isEqualTo(10);
    assertThat(deserialized.getLastEventTime()).isNull();
  }

  @Test
  void testSerializeMultipleSplits() throws Exception {
    EventLogSourceSplit split1 = new EventLogSourceSplit(0, "uuid-1");
    EventLogSourceSplit split2 = new EventLogSourceSplit(15, "uuid-2");
    EventLogSourceSplit split3 = new EventLogSourceSplit(7, null);

    byte[] serialized1 = serializer.serialize(split1);
    byte[] serialized2 = serializer.serialize(split2);
    byte[] serialized3 = serializer.serialize(split3);

    EventLogSourceSplit deserialized1 =
        serializer.deserialize(serializer.getVersion(), serialized1);
    EventLogSourceSplit deserialized2 =
        serializer.deserialize(serializer.getVersion(), serialized2);
    EventLogSourceSplit deserialized3 =
        serializer.deserialize(serializer.getVersion(), serialized3);

    assertThat(deserialized1).isEqualTo(split1);
    assertThat(deserialized2).isEqualTo(split2);
    assertThat(deserialized3).isEqualTo(split3);
  }

  @Test
  void testDeserializeWithWrongVersion() {
    EventLogSourceSplit split = new EventLogSourceSplit(1, "test");

    assertThatThrownBy(
            () -> {
              byte[] serialized = serializer.serialize(split);
              serializer.deserialize(999, serialized); // Wrong version
            })
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Unsupported version");
  }

  @Test
  void testSerializeWithLongUUID() throws Exception {
    String longUuid = "550e8400-e29b-41d4-a716-446655440000-with-extra-data";
    EventLogSourceSplit original = new EventLogSourceSplit(3, longUuid);

    byte[] serialized = serializer.serialize(original);
    EventLogSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized.getLastEventTime()).isEqualTo(longUuid);
  }
}
