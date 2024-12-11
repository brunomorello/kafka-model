package pt.bmo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    private static final String TOPIC_NAME = "test-topic";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> kafkaProducer) {
        this.kafkaProducer = new KafkaProducer<>(kafkaProducer);
    }

    public static Map<String, Object> propertiesMap() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        );
    }

    public void publishMsgSync(final String key, final String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, key, value);

        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            LOGGER.info("partition: {} - offset: {}", recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            LOGGER.error("error: {}", e.getMessage());
        }
    }

    public void publishMsgAsync(final String key, final String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, key, value);
        Callback callback = (recordMetadata, e) -> {
            if (Objects.nonNull(e)) {
                LOGGER.error("Exception during publish async: {}", e.getMessage());
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
        MessageProducer messageProducer = new MessageProducer(propertiesMap());
//        messageProducer.publishMsgSync(null, "ALO");
        messageProducer.publishMsgSync("1", "ALO1");
        messageProducer.publishMsgSync(null, "ALO14");
        messageProducer.publishMsgSync("3", "ALO13");
        messageProducer.publishMsgSync("1", "ALO2");
//        messageProducer.publishMsgAsync("1", "TST-ASYNC1");
//        messageProducer.publishMsgAsync("2", "TST-ASYNC2");
//        messageProducer.publishMsgAsync(null, "TST-ASYNC3");
//        messageProducer.publishMsgAsync("1", "TST-ASYNC4");
        Thread.sleep(5000);
    }
}
