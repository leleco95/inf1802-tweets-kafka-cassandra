import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TweetRepository {

    private static final String TABLE_NAME = "tweets";
    private static final String TABLE_BY_USER_NAME = TABLE_NAME + "ByUser";
    private Session session;
    public TweetRepository(Session session) { this.session = session; }

    public void createTable() {
        System.out.println("createTable ---- init");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("id uuid PRIMARY KEY,")
                .append("user text,")
                .append("message text,")
                .append("date date,")
                .append("source text,")
                .append("truncated Boolean,")
                .append("latitude double,")
                .append("longitude double,")
                .append("favorited Boolean,")
                .append("contributors list<bigint>);");

        final String query = sb.toString();
        System.out.println(query);
        session.execute(query);
        System.out.println("createTable ---- end");
    }

    public void createTableTweetsByUser() {
        // Foi escolhido o usuário pois é um bom jeito de particionar os tweets.
        // O ideal provavelmente seria utilizar o ID fornecido pelo twitter, já que o nome do usuário
        // pode ser alterado.
        // Outra opção de primary key é a data, se o objetivo for buscar por datas.
        // Acredito que as outras informações não são muito úteis para particionar,
        // a menos que fosse utilizada a informação de "Place" (dependendo de como fosse fornecido).
        System.out.println("createTableTweetsByUser ---- init");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_BY_USER_NAME).append("(")
                .append("id uuid,")
                .append("user text,")
                .append("message text,")
                .append("date date,")
                .append("source text,")
                .append("truncated Boolean,")
                .append("latitude double,")
                .append("longitude double,")
                .append("favorited Boolean,")
                .append("contributors list<bigint>,")
                .append("PRIMARY KEY (user, id));");

        final String query = sb.toString();
        System.out.println(query);
        session.execute(query);
        System.out.println("createTableTweetsByUser ---- end");
    }

    public void insertTweet(Tweet tweet) {
        System.out.println("insertTweet ---- init");

        String contributors = "";
        String contributorsList = "";
        if(tweet.getContributors() != null && !tweet.getContributors().isEmpty()) {
            contributors = ", contributors";
            contributorsList = ", " + tweet.getContributors().toString();
        }

        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME).append("(id, user, message, date, source, truncated, latitude, longitude, favorited").append(contributors).append(") ")
                .append("VALUES (").append(tweet.getId()).append(", '")
                .append(tweet.getUser()).append("', '")
                .append(tweet.getMessage()).append("', '")
                .append(tweet.getDate()).append("', '")
                .append(tweet.getSource()).append("', ")
                .append(tweet.isTruncated()).append(", ")
//                .append(tweet.getGeoLocation().getLatitude()).append("', '")
//                .append(tweet.getGeoLocation().getLongitude()).append("', '")
                .append(tweet.getLatitude()).append(", ")
                .append(tweet.getLongitude()).append(", ")
                .append(tweet.isFavorited())
                .append(contributorsList).append(");");

        final String query = sb.toString();
        System.out.println(query);
        session.execute(query);
        System.out.println("insertTweet ---- end");
    }

    public void insertTweetByUser(Tweet tweet) {
        System.out.println("insertTweetByUser ---- init");

        String contributors = "";
        String contributorsList = "";
        if(tweet.getContributors() != null && !tweet.getContributors().isEmpty()) {
            contributors = ", contributors";
            contributorsList = ", " + tweet.getContributors().toString();
        }

        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_BY_USER_NAME).append("(id, user, message, date, source, truncated, latitude, longitude, favorited").append(contributors).append(") ")
                .append("VALUES (").append(tweet.getId()).append(", '")
                .append(tweet.getUser()).append("', '")
                .append(tweet.getMessage()).append("', '")
                .append(tweet.getDate()).append("', '")
                .append(tweet.getSource()).append("', ")
                .append(tweet.isTruncated()).append(", ")
//                .append(tweet.getGeoLocation().getLatitude()).append("', '")
//                .append(tweet.getGeoLocation().getLongitude()).append("', '")
                .append(tweet.getLatitude()).append(", ")
                .append(tweet.getLongitude()).append(", ")
                .append(tweet.isFavorited())
                .append(contributorsList).append(");");

        final String query = sb.toString();
        System.out.println(query);
        session.execute(query);
        System.out.println("insertTweetByUser ---- end");
    }

    public List<Tweet> selectAll(String tableName) {
        System.out.println("selectAll ---- init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(tableName);

        final String query = sb.toString();
        System.out.println(query);
        ResultSet rs = session.execute(query);
        List<Tweet> tweets = new ArrayList<>();

        for(Row r : rs) {
            Tweet tweet = new Tweet(r.getUUID("id"), r.getString("user"), r.getString("message"), r.getDate("date"), r.getString("source"), r.getBool("truncated"), r.getDouble("latitude"), r.getDouble("longitude"), r.getBool("favorited"), r.getList("contributors", Long.class));
            System.out.println("Tweet = " + tweet.getId() + ", "
                    + tweet.getUser() + ", "
                    + tweet.getMessage() + ", "
                    + tweet.getDate() + ", "
                    + tweet.getSource() + ", "
                    + tweet.isTruncated() + ", "
                    + tweet.getLatitude() + ", "
                    + tweet.getLongitude() + ", "
                    + tweet.isFavorited() + ", "
                    + tweet.getContributors());
            tweets.add(tweet);
        }

        System.out.println("selectAll ---- end");
        return tweets;
    }

    public Tweet selectByUser(String user) {
        System.out.println("selectByUser ---- init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_BY_USER_NAME)
                .append(" WHERE user = '").append(user).append("';");

        final String query = sb.toString();
        System.out.println(query);
        ResultSet rs = session.execute(query);
        List<Tweet> tweets = new ArrayList<>();

        for(Row r : rs) {
            Tweet tweet = new Tweet(r.getUUID("id"), r.getString("user"), r.getString("message"), r.getDate("date"), r.getString("source"), r.getBool("truncated"), r.getDouble("latitude"), r.getDouble("longitude"), r.getBool("favorited"), r.getList("contributors", Long.class));
            tweets.add(tweet);
        }

        Tweet tweet = tweets.get(0);
        System.out.println("Tweet = " + tweet.getId() + ", "
                + tweet.getUser() + ", "
                + tweet.getMessage() + ", "
                + tweet.getDate() + ", "
                + tweet.getSource() + ", "
                + tweet.isTruncated() + ", "
                + tweet.getLatitude() + ", "
                + tweet.getLongitude() + ", "
                + tweet.isFavorited() + ", "
                + tweet.getContributors());

        System.out.println("selectByUser ---- end");
        return tweets.get(0);
    }

    public void deleteTweet(UUID id) {
        System.out.println("deleteTweet ---- init");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME)
                .append(" WHERE id = ")
                .append(id.toString()).append(";");

        final String query = sb.toString();
        System.out.println(query);
        session.execute(query);
        System.out.println("deleteTweet ---- end");
    }

    public void deleteTweetByUser(String user) {
        System.out.println("deleteTweetByUser ---- init");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_BY_USER_NAME)
                .append(" WHERE user = '")
                .append(user).append("';");

        final String query = sb.toString();
        System.out.println(query);
        session.execute(query);
        System.out.println("deleteTweetByUser ---- end");
    }

    public void deleteTable(String tableName) {
        System.out.println("deleteTable ---- init");
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);

        final String query = sb.toString();
        System.out.println(query);
        session.execute(query);
        System.out.println("deleteTable ---- end");
    }
}
