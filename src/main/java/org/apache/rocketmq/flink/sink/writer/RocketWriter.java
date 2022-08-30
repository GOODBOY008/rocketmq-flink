package org.apache.rocketmq.flink.sink.writer;

import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommitable;

import java.io.IOException;
import java.util.Collection;

/**
 * @author: Aiden.gong
 * @since: 2022/8/27 15:58
 */
public class RocketWriter<IN> implements PrecommittingSinkWriter<IN, RocketMQCommitable> {
    @Override
    public Collection<RocketMQCommitable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {

    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
