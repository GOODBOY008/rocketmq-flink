package org.apache.rocketmq.flink.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommittable;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommittableSerializer;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommitter;
import org.apache.rocketmq.flink.sink.writer.RocketWriter;

import java.io.IOException;

public class RocketMQSink<IN> implements TwoPhaseCommittingSink<IN, RocketMQCommittable> {
    @Override
    public PrecommittingSinkWriter<IN, RocketMQCommittable> createWriter(InitContext context) throws IOException {
        return new RocketWriter<>(deliveryGuarantee);
    }

    @Override
    public Committer<RocketMQCommittable> createCommitter() throws IOException {
        return new RocketMQCommitter();
    }

    @Override
    public SimpleVersionedSerializer<RocketMQCommittable> getCommittableSerializer() {
        return new RocketMQCommittableSerializer();
    }
}
