package nb.kafka.operator;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer; // NOSONAR

@SuppressWarnings("restriction")
public final class HealthServer {
  private final KafkaOperator operator;

  public HealthServer(KafkaOperator operator) {
    this.operator = operator;
  }

  private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);

  public void start(int port) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/healthy", exchange -> exchange.sendResponseHeaders(200, -1));
    server.createContext("/ready", exchange -> {
      if (operator.checkOperatorState()) {
        exchange.sendResponseHeaders(200, -1);
      } else {
        exchange.sendResponseHeaders(500, -1);
      }
    });
    
    logger.info("Starting health checks server on port: {}", port);
    server.start();
  }
}