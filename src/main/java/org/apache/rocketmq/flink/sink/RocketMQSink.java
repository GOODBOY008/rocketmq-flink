package org.apache.rocketmq.flink.sink;

import org.apache.rocketmq.flink.sink.committer.RocketMQCommittable;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommittableSerializer;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommitter;
import org.apache.rocketmq.flink.sink.writer.RocketMQWriter;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class RocketMQSink<IN>
        implements StatefulSink<IN, RocketMQWriterState>,
                TwoPhaseCommittingSink<IN, RocketMQCommittable> {
    private final DeliveryGuarantee deliveryGuarantee;

    private final Properties rabbitmqProducerConfig;

    RocketMQSink(DeliveryGuarantee deliveryGuarantee, Properties rabbitmqProducerConfig) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.rabbitmqProducerConfig = rabbitmqProducerConfig;
    }

    /**
     * Create a {@link RocketMQSinkBuilder} to construct a new {@link RocketMQSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link RocketMQSinkBuilder}
     */
    public static <IN> RocketMQSinkBuilder<IN> builder() {
        return new RocketMQSinkBuilder<>();
    }

    @Override
    public RocketMQWriter<IN> createWriter(InitContext context) throws IOException {
        return new RocketMQWriter<IN>(
                context, rabbitmqProducerConfig, deliveryGuarantee, Collections.emptyList());
    }

    @Override
    public RocketMQWriter<IN> restoreWriter(
            InitContext context, Collection<RocketMQWriterState> recoveredState)
            throws IOException {
        return new RocketMQWriter<IN>(
                context, rabbitmqProducerConfig, deliveryGuarantee, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<RocketMQWriterState> getWriterStateSerializer() {
        return new RocketMQWriterStateSerializer();
    }

    @Override
    public Committer<RocketMQCommittable> createCommitter() throws IOException {
        return new RocketMQCommitter(rabbitmqProducerConfig);
    }

    @Override
    public SimpleVersionedSerializer<RocketMQCommittable> getCommittableSerializer() {
        return new RocketMQCommittableSerializer();
    }
}
