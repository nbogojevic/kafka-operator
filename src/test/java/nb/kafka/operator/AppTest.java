package nb.kafka.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AppTest {
  @Test
  void testOperatorStart() throws Exception {
    AppConfig config = Main.loadConfig();
    AppConfig defaultConfig = AppConfig.defaultConfig();

    assertEquals(defaultConfig.getBootstrapServers(), config.getBootstrapServers());
    assertEquals(defaultConfig.getOperatorId(), config.getOperatorId());
    assertEquals(defaultConfig.getDefaultReplicationFactor(), config.getDefaultReplicationFactor());
    assertEquals(defaultConfig.isEnabledTopicImport(), config.isEnabledTopicImport());
    assertEquals(defaultConfig.isEnabledAclManagement(), config.isEnabledAclManagement());
    assertEquals(defaultConfig.getUsernamePoolSecretName(), config.getUsernamePoolSecretName());
    assertEquals(defaultConfig.getConsumedUsersSecretName(), config.getConsumedUsersSecretName());
    assertEquals(defaultConfig.getMetricsPort(), config.getMetricsPort());
    assertEquals(defaultConfig.getHealthsPort(), config.getHealthsPort());
    assertEquals(defaultConfig.getKafkaTimeoutMs(), config.getKafkaTimeoutMs());
    assertEquals(defaultConfig.getMaxReplicationFactor(), config.getMaxReplicationFactor());
    assertEquals(defaultConfig.getMaxPartitions(), config.getMaxPartitions());
    assertEquals(defaultConfig.getMaxRetentionMs(), config.getMaxRetentionMs());
  }
}
