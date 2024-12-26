package pt.bmo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.bmo.listeners.RebalancedListener;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerManualAsyncCommitSpecificOffset {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumerManualAsyncCommitSpecificOffset.class);
    private static final String TOPIC_NAME = "test-topic";
    private Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
    KafkaConsumer<String, String> kafkaConsumer;

    public MessageConsumerManualAsyncCommitSpecificOffset() {
        this.kafkaConsumer = new KafkaConsumer<>(buildPropertiesMap());
    }

    private Map<String, Object> buildPropertiesMap() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:49092,localhost:29092,localhost:39092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer",
//                ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer2",
//                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
        );
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(TOPIC_NAME), new RebalancedListener());
        Duration timeoutDuration = Duration.of(100, ChronoUnit.MILLIS);

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(timeoutDuration);
                records.forEach(record -> {
                    LOGGER.info("Consuming record key: {} value: {} partition: {} offset:{}", record.key(), record.value(), record.partition(), record.offset()+1);
                    offsetAndMetadataMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1));
                });
                if (!records.isEmpty()) {
                    kafkaConsumer.commitAsync(offsetAndMetadataMap, (map, e) -> {
                        if (e != null) {
                            LOGGER.error("Exception during async commit: {}", e.getMessage());
                        } else {
                            LOGGER.info("Offset commited - map: {}", offsetAndMetadataMap);
                        }
                    });
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during poll: {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumerManualAsyncCommitSpecificOffset messageConsumer = new MessageConsumerManualAsyncCommitSpecificOffset();
        messageConsumer.pollKafka();
    }
}