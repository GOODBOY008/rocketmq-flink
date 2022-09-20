package org.apache.rocketmq.flink.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RocketMQWriterStateSerializer implements SimpleVersionedSerializer<RocketMQWriterState> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(RocketMQWriterState state) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(state);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public RocketMQWriterState deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            final String transactionalIdPrefx = in.readUTF();
            return new KafkaWriterState(transactionalIdPrefx);
        }
    }
}
