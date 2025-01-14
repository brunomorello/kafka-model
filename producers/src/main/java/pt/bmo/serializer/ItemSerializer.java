package pt.bmo.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.bmo.domain.Item;

public class ItemSerializer implements Serializer<Item> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ItemSerializer.class);
    ObjectMapper om = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Item item) {
        LOGGER.info("Entering in serialize method");
        try {
            return om.writeValueAsBytes(item);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error to write values as bytes for item: {} - error: {}", item, e.getMessage());
            return null;
        }
    }
}
