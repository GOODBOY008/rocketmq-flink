package org.apache.rocketmq.flink.sink;

public class RocketMQSinkBuilder<IN> {

    //    private void sanityCheck() {
    //        checkNotNull(
    //                kafkaProducerConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
    //                "bootstrapServers");
    //        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
    //            checkState(
    //                    transactionalIdPrefix != null,
    //                    "EXACTLY_ONCE delivery guarantee requires a transactionIdPrefix to be set
    // to provide unique transaction names across multiple KafkaSinks writing to the same Kafka
    // cluster.");
    //        }
    //        checkNotNull(recordSerializer, "recordSerializer");
    //    }
    //
    //    public RocketMQSink build() {
    //        sanityCheck();
    //        return new RocketMQSink<>(
    //                deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix,
    // recordSerializer);
    //    }

}
