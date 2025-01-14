package pt.bmo.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.bmo.domain.Item;

import java.io.IOException;

public class ItemDeserializer implements Deserializer<Item> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ItemDeserializer.class);
    ObjectMapper om = new ObjectMapper();

    @Override
    public Item deserialize(String topic, byte[] data) {
        LOGGER.info("entering on deserializer method");
        try {
            return om.readValue(data, Item.class);
        } catch (IOException e) {
            LOGGER.error("error to deserialize item: {}", e.getMessage());
            return null;
        }
    }
}
