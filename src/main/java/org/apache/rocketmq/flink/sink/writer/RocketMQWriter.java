package org.apache.rocketmq.flink.sink.writer;

import org.apache.rocketmq.flink.sink.RocketMQWriterState;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommittable;

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RocketMQWriter<IN>
        implements StatefulSink.StatefulSinkWriter<IN, RocketMQWriterState>,
                PrecommittingSinkWriter<IN, RocketMQCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQWriter.class);

    private final Properties rabbitmqProducerConfig;

    private final DeliveryGuarantee deliveryGuarantee;

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSendErrorsCounter;
    private final Counter numRecordsSendCounter;
    private final Counter numBytesSendCounter;

    public RocketMQWriter(
            InitContext context,
            Properties rabbitmqProducerConfig,
            DeliveryGuarantee deliveryGuarantee,
            Collection<RocketMQWriterState> recoveredState) {

        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        this.rabbitmqProducerConfig =
                checkNotNull(rabbitmqProducerConfig, "rabbitmqProducerConfig");

        this.metricGroup = context.metricGroup();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numRecordsSendErrorsCounter = metricGroup.getNumRecordsSendErrorsCounter();
    }

    @Override
    public Collection<RocketMQCommittable> prepareCommit()
            throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {}

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {}

    @Override
    public void close() throws Exception {}

    @Override
    public List<RocketMQWriterState> snapshotState(long checkpointId) throws IOException {
        return null;
    }
}
