package org.apache.rocketmq.flink.sink.committer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommittable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RocketMQCommittableSerializer implements SimpleVersionedSerializer<RocketMQCommittable> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(RocketMQCommittable state) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeShort(state.getEpoch());
            out.writeLong(state.getProducerId());
            out.writeUTF(state.getTransactionalId());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public RocketMQCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            final short epoch = in.readShort();
            final long producerId = in.readLong();
            final String transactionalId = in.readUTF();
            return new RocketMQCommittable(producerId, epoch, transactionalId);
        }
    }
}
