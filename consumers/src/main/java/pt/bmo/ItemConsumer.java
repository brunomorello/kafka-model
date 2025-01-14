package pt.bmo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.bmo.deserializer.ItemDeserializer;
import pt.bmo.domain.Item;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class ItemConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ItemConsumer.class);
    private static final String TOPIC_NAME = "items";

    KafkaConsumer<Integer, Item> kafkaConsumer;

    public ItemConsumer() {
        this.kafkaConsumer = new KafkaConsumer<>(buildPropertiesMap());
    }

    private Map<String, Object> buildPropertiesMap() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:49092,localhost:29092,localhost:39092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ItemDeserializer.class.getName(),
//                ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer",
                ConsumerConfig.GROUP_ID_CONFIG, "itemsGroupId",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(TOPIC_NAME));
        Duration timeoutDuration = Duration.of(100, ChronoUnit.MILLIS);

        try {
            while (true) {
                ConsumerRecords<Integer, Item> records = kafkaConsumer.poll(timeoutDuration);
                records.forEach(record -> LOGGER.info("Consuming record key: {} value: {} partition: {}", record.key(), record.value(), record.partition()));
            }
        } catch (Exception e) {
            LOGGER.error("Error during poll: {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        ItemConsumer messageConsumer = new ItemConsumer();
        messageConsumer.pollKafka();
    }
}