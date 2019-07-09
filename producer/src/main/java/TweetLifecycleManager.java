import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.glassfish.grizzly.utils.ArrayUtils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class TweetLifecycleManager implements LifecycleManager, Serializable {

    private String _consumerKey;
    private String _consumerSecret;
    private String _accessToken;
    private String _accessTokenSecret;
    private TwitterStreamFactory twitterStreamFactory;
    private KafkaProducer<String,Tweet> producer;

    public TweetLifecycleManager() {
        this._consumerKey = System.getenv().get("TWITTER_CONSUMER_KEY");
        this._consumerSecret = System.getenv().get("TWITTER_CONSUMER_SECRET");
        this._accessToken = System.getenv().get("TWITTER_ACCESS_TOKEN");
        this._accessTokenSecret = System.getenv().get("TWITTER_ACCESS_TOKEN_SECRET");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TweetSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    public void start() {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(this._consumerKey)
                .setOAuthConsumerSecret(this._consumerSecret)
                .setOAuthAccessToken(this._accessToken)
                .setOAuthAccessTokenSecret(this._accessTokenSecret);
        this.twitterStreamFactory = new TwitterStreamFactory(configurationBuilder.build());
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                double latitude = 0;
                double longitude = 0;

                if(status.getGeoLocation() != null) {
                    latitude = status.getGeoLocation().getLatitude();
                    longitude = status.getGeoLocation().getLongitude();
                }

                Tweet t = new Tweet(status.getId(),
                                    status.getUser().getName(),
                                    status.getText(),
                                    status.getCreatedAt(),
                                    status.getSource(),
                                    status.isTruncated(),
                                    latitude,
                                    longitude,
                                    status.isFavorited(),
                                    Arrays.stream(status.getContributors()).boxed().collect(Collectors.toList()));
                ProducerRecord<String, Tweet> record = new ProducerRecord<>("tweets_topic", t);
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("[" + t.getDate() + "]" + t.getUser() + ": " + t.getMessage());
                            System.out.println("Exibindo os meta-dados sobre o envio da mensagem. \n" +
                                    "Topico: " + recordMetadata.topic() + "\n" +
                                    "Partição: " + recordMetadata.partition() + "\n" +
                                    "Offset" + recordMetadata.offset());
                        } else {
                            System.out.println("Erro no envio da mensagem");
                        }
                    }
                });
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatus) {}
            public void onScrubGeo(long l, long l1) {}
            public void onStallWarning(StallWarning stallWarning) {}
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);

        String trackedTerms = "praia";
        FilterQuery query = new FilterQuery();
        query.track(trackedTerms.split(","));
        twitterStream.filter(query);
    }

    public void stop() {
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        twitterStream.shutdown();
    }

}