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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerSeek {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumerSeek.class);
    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "test-topic";
    private Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    public MessageConsumerSeek() {
        this.kafkaConsumer = new KafkaConsumer<>(buildConsumerProperties());
    }

    public Map<String, Object> buildConsumerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:49092,localhost:29092,localhost:39092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(topicName), new RebalancedListener(kafkaConsumer));
        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach(record -> {
                    LOGGER.info("Consumer record is {} and value is {} - partition is {} - offset {}", record.key(), record.value(), record.partition(), record.offset());
                    offsetMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1));
                });

                if (!consumerRecords.isEmpty()) {
//                    kafkaConsumer.commitSync(offsetMap);
                    writeFileSystemOffset(offsetMap);
                    LOGGER.info("Offset commited");
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during poll {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    private void writeFileSystemOffset(Map<TopicPartition, OffsetAndMetadata> offsetMap) throws IOException {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;

        try {
            fos = new FileOutputStream(RebalancedListener.SERIALIZED_FILE_PATH);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(offsetMap);
            LOGGER.info("Offset written on filesystem");
        } catch (Exception e) {
            LOGGER.error("Error to write offset: {}", e.getMessage());
        } finally {
            fos.close();
            oos.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumerSeek messageConsumerSeek = new MessageConsumerSeek();
        messageConsumerSeek.pollKafka();
    }
}
