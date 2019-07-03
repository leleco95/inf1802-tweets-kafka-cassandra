import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("tweets/collector")
public class TweetCollectorResource {
     @Inject
     LifecycleManager manager;
    private static final Logger logger = Logger.getLogger(TweetCollectorResource.class.getName());

    /**
     * Inicia o serviço de Coleta de Tweets
     * @return 200 OK no caso de successo ou 500 em caso de falha
     */
    @GET
    public Response start() {
        Response r = null;
        try {
            manager.start();
            r = Response.ok("Coletor de Twitter iniciado")
                    .build();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
            r = Response.serverError().build();
        }
        return r;
    }

    /**
     * Finaliza o serviço de Coleta de Tweets
     * @return 200 OK no caso de successo ou 500 em caso de falha
     */
    @DELETE
    public Response stop() {
        Response r = null;
        try {
            manager.stop();
            r = Response.ok("Coletor de Twitter finalizado")
                    .build();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
            r = Response.serverError().build();
        }
        return r;
    }
}