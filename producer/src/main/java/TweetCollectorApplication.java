import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TweetCollectorApplication {
    private static final Logger logger = Logger.getLogger(TweetCollectorApplication.class.getName());

    private static void start() throws IOException {
        // O hostname está na variável de ambiente HOSTNAME ou assume localhost
        String hostname = Optional.ofNullable(System.getenv("HOSTNAME")).orElse("localhost");
        // A porta está na variável de ambiente PORT ou assume localhost
        String port = Optional.ofNullable(System.getenv("PORT")).orElse("8080");

        // Inicia o servidor HTTP
        URI baseUri = UriBuilder.fromUri("http://" + hostname + "/").port(Integer.parseInt(port)).build();
        ResourceConfig config = new ResourceConfig(TweetCollectorResource.class);
        config.register(new InjectionBinder());
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config);
        logger.log(Level.INFO, "Aplicação está disponível em {0}", baseUri.toString());

        // Adiciona um shutdown hook para terminar o serviço
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.log(Level.INFO, "Finalizando......");
                try {
                    server.shutdownNow();
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, ex, ex::getMessage);
                }
            }
        }));
        server.start();

    }
    public static void main(String[] args) throws Exception {
        start();
    }

}
