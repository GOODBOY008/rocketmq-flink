package org.apache.rocketmq.flink.sink.writer;

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.rocketmq.flink.sink.committer.RocketMQCommittable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

public class RocketWriter<IN> implements PrecommittingSinkWriter<IN, RocketMQCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketWriter.class);

    private final Properties kafkaProducerConfig;


    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSendErrorsCounter;
    @Deprecated
    private final Counter numRecordsOutErrorsCounter;
    private final Counter numRecordsSendCounter;
    private final Counter numBytesSendCounter;

    public RocketWriter(InitContext context) {

        this.metricGroup = context.metricGroup();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsSendErrorsCounter = metricGroup.getNumRecordsSendErrorsCounter();

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
