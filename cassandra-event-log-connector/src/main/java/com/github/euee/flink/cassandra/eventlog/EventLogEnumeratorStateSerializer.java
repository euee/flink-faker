package com.github.euee.flink.cassandra.eventlog;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/** Serializer for EventLogEnumeratorState. */
public class EventLogEnumeratorStateSerializer
    implements SimpleVersionedSerializer<EventLogEnumeratorState> {

  private static final int VERSION = 1;
  private final EventLogSourceSplitSerializer splitSerializer;

  public EventLogEnumeratorStateSerializer() {
    this.splitSerializer = new EventLogSourceSplitSerializer();
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(EventLogEnumeratorState state) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos)) {

      List<EventLogSourceSplit> splits = state.getRemainingSplits();
      out.writeInt(splits.size());

      for (EventLogSourceSplit split : splits) {
        byte[] splitBytes = splitSerializer.serialize(split);
        out.writeInt(splitBytes.length);
        out.write(splitBytes);
      }

      out.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public EventLogEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(bais)) {

      int numSplits = in.readInt();
      List<EventLogSourceSplit> splits = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        int length = in.readInt();
        byte[] splitBytes = new byte[length];
        in.readFully(splitBytes);
        EventLogSourceSplit split =
            splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes);
        splits.add(split);
      }

      return new EventLogEnumeratorState(splits);
    }
  }
}
