package com.github.euee.flink.cassandra;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** Serializer for CassandraSourceSplit. */
public class CassandraSourceSplitSerializer
    implements SimpleVersionedSerializer<CassandraSourceSplit> {

  private static final int CURRENT_VERSION = 1;

  @Override
  public int getVersion() {
    return CURRENT_VERSION;
  }

  @Override
  public byte[] serialize(CassandraSourceSplit split) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(128);
    out.writeUTF(split.splitId());
    out.writeLong(split.getStartToken());
    out.writeLong(split.getEndToken());
    return out.getCopyOfBuffer();
  }

  @Override
  public CassandraSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    if (version != CURRENT_VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    DataInputDeserializer in = new DataInputDeserializer(serialized);
    String splitId = in.readUTF();
    long startToken = in.readLong();
    long endToken = in.readLong();

    return new CassandraSourceSplit(splitId, startToken, endToken);
  }
}
