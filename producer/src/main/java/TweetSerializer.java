import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TweetSerializer implements Serializer<Tweet> {

    ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Tweet tweet) {
        try {
            return mapper.writeValueAsBytes(tweet);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public void close() {
    }
}