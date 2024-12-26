package pt.bmo.listeners;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalancedListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RebalancedListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        LOGGER.info("onPartitionsRevoked called");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        LOGGER.info("onPartitionsAssigned called");
    }
}
