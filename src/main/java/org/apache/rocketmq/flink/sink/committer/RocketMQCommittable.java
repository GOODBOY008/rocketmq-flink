package org.apache.rocketmq.flink.sink.committer;

import org.apache.flink.annotation.Internal;

@Internal
public class RocketMQCommittable {

    private final long producerId;
    private final short epoch;
    private final String transactionalId;

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    public RocketMQCommittable(long producerId, short epoch, String transactionalId) {
        this.producerId = producerId;
        this.epoch = epoch;
        this.transactionalId = transactionalId;
    }
}
