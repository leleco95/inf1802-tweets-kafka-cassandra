import com.datastax.driver.core.LocalDate;

import java.util.List;
import java.util.UUID;

public class Tweet {
    private UUID id;
    private String user;
    private String message;
    private LocalDate date;
    private String source;
    private boolean truncated;
    //    private GeoLocation geoLocation;
    private double latitude;
    private double longitude;
    private boolean favorited;
    private List<Long> contributors;

    public Tweet() {}

    public Tweet(final UUID id, final String user, final String message, final LocalDate date, final String source, final boolean truncated, final double latitude, final double longitude, final boolean favorited, final List<Long> contributors) {
        this.id = id;
        this.user = user;
        this.message = message;
        this.date = date;
        this.source = source;
        this.truncated = truncated;
        this.latitude = latitude;
        this.longitude = longitude;
        this.favorited = favorited;
        this.contributors = contributors;
    }

    public UUID getId() { return id; }
    public String getUser() { return user; }
    public String getMessage() { return message; }
    public LocalDate getDate() { return date; }
    public String getSource() { return source; }
    public boolean isTruncated() { return truncated; }
    //    public GeoLocation getGeoLocation() { return geoLocation; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public boolean isFavorited() { return favorited; }
    public List<Long> getContributors() { return contributors; }

    public void setId(UUID id) { this.id = id; }
    public void setUser(String user) { this.user = user; }
    public void setMessage(String message) { this.message = message; }
    public void setDate(LocalDate date) { this.date = date; }
    public void setSource(String source) { this.source = source; }
    public void setTruncated(boolean truncated) { this.truncated = truncated; }
    //    public void setGeoLocation(GeoLocation geoLocation) { this.geoLocation = geoLocation; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    public void setFavorited(boolean favorited) { this.favorited = favorited; }
    public void setContributors(List<Long> contributors) { this.contributors = contributors; }

    public String toString() { return "[" + date + "]" + user + ": " + message; }
}