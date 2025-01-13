package pt.bmo.listeners;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalancedListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RebalancedListener.class);
    KafkaConsumer<String, String> kafkaConsumer;

    public RebalancedListener(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitionCollection) {
        LOGGER.info("onPartitionsRevoked called");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitionCollection) {
        LOGGER.info("onPartitionsAssigned called");
//        kafkaConsumer.seekToBeginning(partitionCollection);
        kafkaConsumer.seekToEnd(partitionCollection);
    }
}
