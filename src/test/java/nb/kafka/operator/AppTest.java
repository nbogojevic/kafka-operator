package nb.kafka.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AppTest {
  @Test
  void testOperatorStart() throws Exception {
    AppConfig config = Main.loadConfig();
    AppConfig defaultConfig = AppConfig.defaultConfig();

    assertEquals(defaultConfig.getKafkaUrl(), config.getKafkaUrl());

    assertEquals(defaultConfig.getOperatorId(), config.getOperatorId());
    assertEquals(defaultConfig.getDefaultReplicationFactor(), config.getDefaultReplicationFactor());
    assertEquals(defaultConfig.isEnabledTopicImport(), config.isEnabledTopicImport());
    assertEquals(defaultConfig.isEnabledAclManagement(), config.isEnabledAclManagement());
    assertEquals(defaultConfig.getUsernamePoolSecretName(), config.getUsernamePoolSecretName());
    assertEquals(defaultConfig.getConsumedUsersSecretName(), config.getConsumedUsersSecretName());
    assertEquals(defaultConfig.getPrometheusEndpointPort(), config.getPrometheusEndpointPort());

  }
}
