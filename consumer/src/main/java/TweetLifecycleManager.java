import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TweetLifecycleManager implements LifecycleManager {

    private static Logger logger = LoggerFactory.getLogger(TweetLifecycleManager.class.getName());

    private KafkaConsumer<String, Tweet> consumer;
    private boolean consume;
    private Cluster cluster = null;
    private KeyspaceRepository sr;
    private TweetRepository tr;

    public TweetLifecycleManager() {
        // Criar as propriedades do consumidor
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tweets_consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Criar o consumidor
        consumer = new KafkaConsumer<>(properties);

        // Subscrever o consumidor para o nosso(s) tópico(s)
        consumer.subscribe(Collections.singleton("tweets_topic"));
    }

    public void start() {
        consume = true;

        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .build();

        Session session = cluster.connect();

        ResultSet rs = session.execute("select release_version from system.local");
        Row row = rs.one();
        System.out.println(row.getString("release_version"));

        sr = new KeyspaceRepository(session);
        sr.createKeyspace("tweets", "SimpleStrategy", 1);
        sr.useKeyspace("tweets");

        tr = new TweetRepository(session);

        tr.deleteTable("tweetsByUser");
        tr.createTableTweetsByUser();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(consume) {
                    ConsumerRecords<String, Tweet> poll = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord record : poll) {
                        tr.insertTweetByUser((Tweet) record.value());
//                        logger.info(record.topic() + " - " + record.partition() + " - " + record.value());
                    }
                }
            }
        });

        thread.start();
    }

    public void stop() {
        consume = false;
        tr.deleteTable("tweetsByUser");
        sr.deleteKeyspace("tweets");
        if(cluster != null) cluster.close();
    }
}
