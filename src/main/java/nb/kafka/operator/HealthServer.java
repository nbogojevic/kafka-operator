package nb.kafka.operator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import nb.kafka.operator.model.OperatorError;

@SuppressWarnings("restriction")
public class HealthServer {

  private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);

  public static void start(AppConfig config) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(config.getHealthsPort()), 0);
    server.createContext("/healthy", exchange -> exchange.sendResponseHeaders(200, -1));
    server.createContext("/ready", exchange -> {
      if (isKafkaReachable(config)) {
        exchange.sendResponseHeaders(200, -1);
      } else {
        exchange.sendResponseHeaders(500, -1);
      }
    });
    
    logger.info("Starting health checks server on port: {}", config.getHealthsPort());
    server.start();
  }

  private static boolean isKafkaReachable(AppConfig config) {
    try (KafkaAdmin ka = new KafkaAdminImpl(config)) {
      ka.listTopics();
      return true;
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      logger.error(String.format(OperatorError.KAFKA_UNREACHABLE.toString(), config.getBootstrapServers()));
      return false;
    }
  }
}