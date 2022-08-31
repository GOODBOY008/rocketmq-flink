package org.apache.rocketmq.flink.sink.writer;

import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommittable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class RocketWriter<IN> implements PrecommittingSinkWriter<IN, RocketMQCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketWriter.class);
    private final DeliveryGuarantee deliveryGuarantee;

    public RocketWriter(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
    }

    @Override
    public Collection<RocketMQCommittable> prepareCommit() throws IOException, InterruptedException {
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
