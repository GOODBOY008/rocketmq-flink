package org.apache.rocketmq.flink.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommit;

import java.io.IOException;

/**
 * @author: Aiden.gong
 * @since: 2022/8/27 15:46
 */
public class RocketMQSink<IN> implements TwoPhaseCommittingSink<IN, RocketMQCommit> {
    @Override
    public PrecommittingSinkWriter<IN, RocketMQCommit> createWriter(InitContext context) throws IOException {
        return null;
    }

    @Override
    public Committer<RocketMQCommit> createCommitter() throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<RocketMQCommit> getCommittableSerializer() {
        return null;
    }
}
