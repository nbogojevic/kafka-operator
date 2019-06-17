package nb.kafka.operator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

@SuppressWarnings("restriction")
public class HealthServer {

  private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);
  private static final int DEFAULT_PORT = 8080;

  public static void start(AppConfig config) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(DEFAULT_PORT), 0);
    server.createContext("/healthy", exchange -> exchange.sendResponseHeaders(200, -1));
    server.createContext("/ready", exchange -> {
      if (isKafkaAvailable(config)) {
        exchange.sendResponseHeaders(200, -1);
      } else {
        exchange.sendResponseHeaders(500, -1);
      }
    });
    
    logger.info("Starting health checks server on port: " + DEFAULT_PORT);
    server.start();
  }

  private static boolean isKafkaAvailable(AppConfig config) {
    try {
      KafkaAdmin ka = new KafkaAdminImpl(config);
      ka.listTopics();
      return true;
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      return false;
    }
  }
}