package nb.kafka.operator;

import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class HealthServer {

  private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);
  private static final int DEFAULT_PORT = 8080;
  private static final String DEFAULT_HOST = "0.0.0.0";

  public static void start(AppConfig config) {
    PathHandler pathHandler = new PathHandler();
    pathHandler.addPrefixPath("/healthy", exchange -> exchange.setStatusCode(StatusCodes.OK));
    pathHandler.addPrefixPath("/ready", exchange -> {
      if (isKafkaAvailable(config))
        exchange.setStatusCode(StatusCodes.OK);
      else
        exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
    });
    logger.info("Starting health checks server on port: " + DEFAULT_PORT);
    Undertow server = Undertow.builder().addHttpListener(DEFAULT_PORT, DEFAULT_HOST)
      .setHandler(pathHandler).build();
    server.start();
  }

  private static boolean isKafkaAvailable(AppConfig config) {
    try {
      KafkaAdmin ka = new KafkaAdminImpl(config.getKafkaUrl(), config.getSecurityProtocol());
      ka.listTopics();
      return true;
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      return false;
    }
  }
}