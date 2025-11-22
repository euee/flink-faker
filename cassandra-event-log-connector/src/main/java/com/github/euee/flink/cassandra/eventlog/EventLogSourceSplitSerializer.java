package com.github.euee.flink.cassandra.eventlog;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.*;
import java.nio.charset.StandardCharsets;

/** Serializer for EventLogSourceSplit. */
public class EventLogSourceSplitSerializer
    implements SimpleVersionedSerializer<EventLogSourceSplit> {

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(EventLogSourceSplit split) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos)) {

      out.writeInt(split.getShardId());

      // Write lastEventTime (nullable)
      String lastEventTime = split.getLastEventTime();
      if (lastEventTime == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        byte[] bytes = lastEventTime.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
      }

      out.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public EventLogSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(bais)) {

      int shardId = in.readInt();

      // Read lastEventTime (nullable)
      String lastEventTime = null;
      boolean hasLastEventTime = in.readBoolean();
      if (hasLastEventTime) {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        lastEventTime = new String(bytes, StandardCharsets.UTF_8);
      }

      return new EventLogSourceSplit(shardId, lastEventTime);
    }
  }
}
