package com.example.connector;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * Serializer for ConnectorSourceSplit.
 *
 * <p>This is used for checkpointing and transferring splits between Job Manager and Task Managers.
 *
 * <p>TODO: Update serialization logic if you modify ConnectorSourceSplit fields
 */
public class ConnectorSourceSplitSerializer
    implements SimpleVersionedSerializer<ConnectorSourceSplit> {

  private static final int CURRENT_VERSION = 1;

  @Override
  public int getVersion() {
    return CURRENT_VERSION;
  }

  @Override
  public byte[] serialize(ConnectorSourceSplit split) throws IOException {
    // TODO: Serialize all fields of your split
    DataOutputSerializer out = new DataOutputSerializer(128);
    out.writeUTF(split.splitId());
    out.writeLong(split.getStartRecord());
    out.writeLong(split.getEndRecord());
    return out.getCopyOfBuffer();
  }

  @Override
  public ConnectorSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    if (version != CURRENT_VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    // TODO: Deserialize all fields to recreate your split
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    String splitId = in.readUTF();
    long startRecord = in.readLong();
    long endRecord = in.readLong();

    return new ConnectorSourceSplit(splitId, startRecord, endRecord);
  }
}
