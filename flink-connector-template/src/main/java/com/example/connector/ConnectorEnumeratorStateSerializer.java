package com.example.connector;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for ConnectorEnumeratorState.
 *
 * <p>This is used for checkpointing the split enumerator state.
 *
 * <p>TODO: Update if you add fields to ConnectorEnumeratorState
 */
public class ConnectorEnumeratorStateSerializer
    implements SimpleVersionedSerializer<ConnectorEnumeratorState> {

  private static final int CURRENT_VERSION = 1;
  private final ConnectorSourceSplitSerializer splitSerializer =
      new ConnectorSourceSplitSerializer();

  @Override
  public int getVersion() {
    return CURRENT_VERSION;
  }

  @Override
  public byte[] serialize(ConnectorEnumeratorState state) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(256);

    // Serialize next split ID
    out.writeLong(state.getNextSplitId());

    // Serialize pending splits
    List<ConnectorSourceSplit> pendingSplits = state.getPendingSplits();
    out.writeInt(pendingSplits.size());
    for (ConnectorSourceSplit split : pendingSplits) {
      byte[] splitBytes = splitSerializer.serialize(split);
      out.writeInt(splitBytes.length);
      out.write(splitBytes);
    }

    return out.getCopyOfBuffer();
  }

  @Override
  public ConnectorEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    if (version != CURRENT_VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    DataInputDeserializer in = new DataInputDeserializer(serialized);

    // Deserialize next split ID
    long nextSplitId = in.readLong();

    // Deserialize pending splits
    int numSplits = in.readInt();
    List<ConnectorSourceSplit> pendingSplits = new ArrayList<>(numSplits);
    for (int i = 0; i < numSplits; i++) {
      int splitLength = in.readInt();
      byte[] splitBytes = new byte[splitLength];
      in.read(splitBytes);
      ConnectorSourceSplit split =
          splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes);
      pendingSplits.add(split);
    }

    return new ConnectorEnumeratorState(pendingSplits, nextSplitId);
  }
}
