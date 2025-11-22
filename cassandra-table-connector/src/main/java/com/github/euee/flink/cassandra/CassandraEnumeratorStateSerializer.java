package com.github.euee.flink.cassandra;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for CassandraEnumeratorState. */
public class CassandraEnumeratorStateSerializer
    implements SimpleVersionedSerializer<CassandraEnumeratorState> {

  private static final int CURRENT_VERSION = 1;
  private final CassandraSourceSplitSerializer splitSerializer =
      new CassandraSourceSplitSerializer();

  @Override
  public int getVersion() {
    return CURRENT_VERSION;
  }

  @Override
  public byte[] serialize(CassandraEnumeratorState state) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(256);

    List<CassandraSourceSplit> pendingSplits = state.getPendingSplits();
    out.writeInt(pendingSplits.size());
    for (CassandraSourceSplit split : pendingSplits) {
      byte[] splitBytes = splitSerializer.serialize(split);
      out.writeInt(splitBytes.length);
      out.write(splitBytes);
    }

    return out.getCopyOfBuffer();
  }

  @Override
  public CassandraEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    if (version != CURRENT_VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    DataInputDeserializer in = new DataInputDeserializer(serialized);

    int numSplits = in.readInt();
    List<CassandraSourceSplit> pendingSplits = new ArrayList<>(numSplits);
    for (int i = 0; i < numSplits; i++) {
      int splitLength = in.readInt();
      byte[] splitBytes = new byte[splitLength];
      in.read(splitBytes);
      CassandraSourceSplit split =
          splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes);
      pendingSplits.add(split);
    }

    return new CassandraEnumeratorState(pendingSplits);
  }
}
