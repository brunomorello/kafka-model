package pt.bmo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.bmo.domain.Item;
import pt.bmo.serializer.ItemSerializer;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class ItemProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ItemProducer.class);

    private static final String TOPIC_NAME = "items";
    KafkaProducer<Integer, Item> kafkaProducer;

    public ItemProducer(Map<String, Object> kafkaProducer) {
        this.kafkaProducer = new KafkaProducer<>(kafkaProducer);
    }

    public static Map<String, Object> propertiesMap() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:49092,localhost:29092,localhost:39092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class.getName()
        );
    }

    public void publishMsgSync(final Item item) {
        ProducerRecord<Integer, Item> producerRecord = new ProducerRecord<>(TOPIC_NAME, item.getId(), item);
        // or using Integer, String and serialize/deserialize the string to JSON

        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            LOGGER.info("Message {} sent successfully with key: {}", item, item.getId());
            LOGGER.info("partition: {} - offset: {}", recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            LOGGER.error("error: {}", e);
        }
    }

    public void publishMsgAsync(final Item item) {
        ProducerRecord<Integer, Item> producerRecord = new ProducerRecord<>(TOPIC_NAME, item.getId(), item);
        Callback callback = (recordMetadata, e) -> {
            if (Objects.nonNull(e)) {
                LOGGER.error("Exception during publish async: {}", e);
            } else {
                LOGGER.info("Published message offset in callback is {} - on partition {}", recordMetadata.offset(), recordMetadata.partition());
            }
        };
        kafkaProducer.send(producerRecord, callback);
    }

    public void close(){
        kafkaProducer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        ItemProducer itemProducer = new ItemProducer(propertiesMap());
//        messageProducer.publishMsgSync(null, "ALO");
//        messageProducer.publishMsgSync("1", "ALO1");
//        messageProducer.publishMsgSync(null, "ALO14");
//        messageProducer.publishMsgSync("3", "ALO13");
//        messageProducer.publishMsgSync("1", "ALO2");
//        messageProducer.publishMsgAsync("1", "TST-ASYNC1");
//        messageProducer.publishMsgAsync("2", "TST-ASYNC2");
//        messageProducer.publishMsgAsync(null, "TST-ASYNC3");
//        messageProducer.publishMsgAsync("1", "TST-ASYNC4");
        Item tvLg = new Item(1, "TV LG", 1000.01);
        Item ps5 = new Item(2, "PS5", 502.99);

        itemProducer.publishMsgSync(tvLg);
        itemProducer.publishMsgSync(ps5);
        Thread.sleep(3000);
    }
}
