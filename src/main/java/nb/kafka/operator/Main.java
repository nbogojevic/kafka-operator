package nb.kafka.operator;

import static nb.kafka.operator.util.PropertyUtil.getSystemPropertyOrEnvVar;
import static nb.kafka.operator.util.PropertyUtil.isBlank;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;
import nb.kafka.operator.util.PropertyUtil;

public final class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException {
    AppConfig config = loadConfig();
    setupJmxRegistry(config.getOperatorId());
    Runnable stopHttpServer = setupPrometheusRegistry(config.getPrometheusEndpointPort());

    HealthServer.start(config);
    KafkaOperator operator = new KafkaOperator(config);

    Runtime.getRuntime().addShutdownHook(new Thread(operator::shutdown));
    Runtime.getRuntime().addShutdownHook(new Thread(stopHttpServer));

    if (config.isEnabledTopicImport()) {
      log.debug("Importing topics");
      operator.importTopics();
    }
    operator.watch();
    log.info("Operator {} started: Managing cluster {}", config.getOperatorId(), config.getKafkaUrl());
  }

  public static AppConfig loadConfig() {
    AppConfig config = new AppConfig();
    AppConfig defaultConfig = AppConfig.defaultConfig();

    config.setKafkaUrl(getSystemPropertyOrEnvVar("bootstrap.server", defaultConfig.getKafkaUrl()));
    config.setOperatorId(getSystemPropertyOrEnvVar("operator.id", defaultConfig.getOperatorId()));
    config.setDefaultReplicationFactor(
        getSystemPropertyOrEnvVar("default.replication.factor", defaultConfig.getDefaultReplicationFactor()));
    config.setEnableTopicImport(getSystemPropertyOrEnvVar("import.topics", defaultConfig.isEnabledTopicImport()));
    config.setEnableAclManagement(getSystemPropertyOrEnvVar("enable.acl", defaultConfig.isEnabledAclManagement()));
    config.setSecurityProtocol(getSystemPropertyOrEnvVar("security.protocol", ""));
    if (config.isEnabledAclManagement() && isBlank(config.getSecurityProtocol())) {
      config.setSecurityProtocol("SASL_PLAINTEXT");
      log.warn("ACL was enabled, but not security.protocol, forcing security protocol to {}",
          config.getSecurityProtocol());
    }
    config.setStandardLabels(PropertyUtil.stringToMap(getSystemPropertyOrEnvVar("standard.labels", "")));
    config.setStandardAclLabels(PropertyUtil.stringToMap(getSystemPropertyOrEnvVar("standard.acl.labels", "")));
    config.setUsernamePoolSecretName(
        getSystemPropertyOrEnvVar("username.pool.secret", defaultConfig.getUsernamePoolSecretName()));
    config.setConsumedUsersSecretName(
        getSystemPropertyOrEnvVar("consumed.usernames.secret", defaultConfig.getConsumedUsersSecretName()));
    config.setPrometheusEndpointPort(
        getSystemPropertyOrEnvVar("prometheus.port", defaultConfig.getPrometheusEndpointPort()));
    config.setKafkaTimeoutMs(getSystemPropertyOrEnvVar("kafka.timeout.ms", defaultConfig.getKafkaTimeoutMs()));

    log.debug("Loaded config, {}", config);
    return config;
  }

  public static void setupJmxRegistry(String operatorId) {
    Metrics.addRegistry(new JmxMeterRegistry(new JmxConfig() {
      @Override
      public String get(String key) {
        return null;
      }

      @Override
      public String domain() {
        return operatorId;
      }
    }, Clock.SYSTEM));
  }

  public static Runnable setupPrometheusRegistry(int port) {
    PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    Metrics.addRegistry(prometheusRegistry);

    // the registry exposes only a scrape method for building prometheus-formatted data
    // we have to deal with the HTTP endpoint
    try {
      HTTPServer server = new HTTPServer(new InetSocketAddress(port), prometheusRegistry.getPrometheusRegistry(), true);
      return server::stop;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
