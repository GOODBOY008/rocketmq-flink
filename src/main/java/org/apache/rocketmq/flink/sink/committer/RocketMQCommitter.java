package org.apache.rocketmq.flink.sink.committer;


import org.apache.flink.api.connector.sink2.Committer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Committer implementation for {@link org.apache.rocketmq.flink.sink.RocketMQSink}.
 *
 * <p>The committer is responsible to finalize the Pulsar transactions by committing them.
 */
public class RocketMQCommitter implements Committer<RocketMQCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQCommitter.class);

    @Override
    public void commit(Collection<CommitRequest<RocketMQCommittable>> requests) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
