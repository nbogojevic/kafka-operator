package nb.kafka.operator;

import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;

public class HealthServer {

  private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);
  private static final int DEFAULT_PORT = 8080;
  private static final String DEFAULT_HOST = "0.0.0.0";

  public static void start(){
    PathHandler pathHandler = new PathHandler();
    pathHandler.addPrefixPath("/healthy", new HttpHandler() {
      public void handleRequest(HttpServerExchange exchange) throws Exception {
        exchange.setStatusCode(StatusCodes.OK);
      }
    });
    pathHandler.addPrefixPath("/ready", new HttpHandler() {
      public void handleRequest(HttpServerExchange exchange) throws Exception {
        //TODO check connection to Kafka Cluster
        exchange.setStatusCode(StatusCodes.OK);
      }
    });
    logger.info("Starting health checks server on port: " + DEFAULT_PORT);
    Undertow server = Undertow.builder().addHttpListener(DEFAULT_PORT, DEFAULT_HOST)
      .setHandler(pathHandler).build();
    server.start();
  }
}