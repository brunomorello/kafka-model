package pt.bmo.listeners;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RebalancedListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RebalancedListener.class);
    KafkaConsumer<String, String> kafkaConsumer;
    public static final String SERIALIZED_FILE_PATH = "consumers/src/main/resources/offset.ser";

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
//        kafkaConsumer.seekToEnd(partitionCollection);

        Map<TopicPartition, OffsetAndMetadata> fsOffsetMap = readOffsetSerializationFile();
        LOGGER.info("Filesystem offset map: {}", fsOffsetMap);

        if (!fsOffsetMap.isEmpty()) {
            partitionCollection.forEach(topicPartition -> kafkaConsumer.seek(topicPartition, fsOffsetMap.get(topicPartition)));
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> readOffsetSerializationFile() {
        Map<TopicPartition, OffsetAndMetadata> fsOffsetMap = new HashMap<>();
        FileInputStream fis;
        BufferedInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            fis = new FileInputStream(SERIALIZED_FILE_PATH);
            bis = new BufferedInputStream(fis);
            ois = new ObjectInputStream(bis);
            fsOffsetMap = (Map<TopicPartition, OffsetAndMetadata>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
        } finally {
            try {
                ois.close();
                bis.close();
                bis.close();
            } catch (IOException e) {
                LOGGER.error("Error to close resource: {}", e.getMessage());
            }
        }
        return fsOffsetMap;
    }
}
