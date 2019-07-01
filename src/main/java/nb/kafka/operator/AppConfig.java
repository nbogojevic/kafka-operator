package nb.kafka.operator;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * Hold the application configuration.
 */
public class AppConfig {
  private String bootstrapServers;
  private String securityProtocol;
  private short defaultReplicationFactor;
  private boolean enableTopicDelete;
  private boolean enableTopicImport;
  private boolean enableAclManagement;
  private String operatorId;
  private Map<String, String> standardLabels;
  private String usernamePoolSecretName;
  private String consumedUsersSecretName;
  private Map<String, String> standardAclLabels;
  private int metricsPort;
  private int healthsPort;
  private int kafkaTimeoutMs;
  private short maxReplicationFactor;
  private int maxPartitions;
  private long maxRetentionMs;

  private static AppConfig defaultConfig;

  /**
   * Return the default configuration. Mutating the returned object has no effect.
   */
  public static final AppConfig defaultConfig() {
    if (defaultConfig == null) {
      AppConfig conf = new AppConfig();
      conf.bootstrapServers = "kafka:9092";
      conf.securityProtocol = "";
      conf.defaultReplicationFactor = (short)1;
      conf.enableTopicDelete = false;
      conf.enableTopicImport = false;
      conf.enableAclManagement = false;
      conf.operatorId = "kafka-topic-operator";
      conf.standardLabels = new HashMap<>();
      conf.usernamePoolSecretName = "kafka-cluster-kafka-auth-pool";
      conf.consumedUsersSecretName = "kafka-cluster-kafka-consumed-auth-pool";
      conf.standardAclLabels = new HashMap<>();
      conf.metricsPort = 9889;
      conf.healthsPort = 9559;
      conf.kafkaTimeoutMs = 30000;
      conf.maxReplicationFactor = (short)3;
      conf.maxPartitions = 2000;
      conf.maxRetentionMs = Duration.of(7, ChronoUnit.DAYS).toMillis();
      defaultConfig = conf;
    }
    return new AppConfig(defaultConfig);
  }

  public AppConfig() {
  }

  public AppConfig(AppConfig config) {
    this.setBootstrapServers(config.getBootstrapServers());
    this.setSecurityProtocol(config.getSecurityProtocol());
    this.setDefaultReplicationFactor(config.getDefaultReplicationFactor());
    this.setEnableTopicDelete(config.isEnabledTopicDelete());
    this.setEnableTopicImport(config.isEnabledTopicImport());
    this.setEnableAclManagement(config.isEnabledAclManagement());
    this.setOperatorId(config.getOperatorId());
    this.setStandardLabels(config.getStandardLabels());
    this.setStandardAclLabels(config.getStandardAclLabels());
    this.setUsernamePoolSecretName(config.getUsernamePoolSecretName());
    this.setConsumedUsersSecretName(config.getConsumedUsersSecretName());
    this.setMetricsPort(config.getMetricsPort());
    this.setHealthsPort(config.getHealthsPort());
    this.setKafkaTimeoutMs(config.getKafkaTimeoutMs());
    this.setMaxReplicationFactor(config.getMaxReplicationFactor());
    this.setMaxPartitions(config.getMaxPartitions());
    this.setMaxRetentionMs(config.getMaxRetentionMs());
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }
  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }
  public String getSecurityProtocol() {
    return securityProtocol;
  }
  public void setSecurityProtocol(String securityProtocol) {
    this.securityProtocol = securityProtocol;
  }
  public short getDefaultReplicationFactor() {
    return defaultReplicationFactor;
  }
  public void setDefaultReplicationFactor(short defaultReplicationFactor) {
    this.defaultReplicationFactor = defaultReplicationFactor;
  }
  public boolean isEnabledTopicDelete() {
    return enableTopicDelete;
  }
  public void setEnableTopicDelete(boolean enableTopicDelete) {
    this.enableTopicDelete = enableTopicDelete;
  }
  public boolean isEnabledTopicImport() {
    return enableTopicImport;
  }
  public void setEnableTopicImport(boolean enableTopicImport) {
    this.enableTopicImport = enableTopicImport;
  }
  public boolean isEnabledAclManagement() {
    return enableAclManagement;
  }
  public void setEnableAclManagement(boolean enableAclManagement) {
    this.enableAclManagement = enableAclManagement;
  }
  public String getOperatorId() {
    return operatorId;
  }
  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
  }
  public Map<String, String> getStandardLabels() {
    return standardLabels;
  }
  public void setStandardLabels(Map<String, String> standardLabels) {
    this.standardLabels = standardLabels;
  }
  public String getUsernamePoolSecretName() {
    return usernamePoolSecretName;
  }
  public void setUsernamePoolSecretName(String usernamePoolSecretName) {
    this.usernamePoolSecretName = usernamePoolSecretName;
  }
  public String getConsumedUsersSecretName() {
    return consumedUsersSecretName;
  }
  public void setConsumedUsersSecretName(String consumedUsersSecretName) {
    this.consumedUsersSecretName = consumedUsersSecretName;
  }
  public Map<String, String> getStandardAclLabels() {
    return standardAclLabels;
  }
  public void setStandardAclLabels(Map<String, String> standardAclLabels) {
    this.standardAclLabels = standardAclLabels;
  }
  public int getMetricsPort() {
    return this.metricsPort;
  }
  public void setMetricsPort(int metricsPort) {
    this.metricsPort = metricsPort;
  }
  public int getHealthsPort() {
    return this.healthsPort;
  }
  public void setHealthsPort(int healthsPort) {
    this.healthsPort = healthsPort;
  }
  public int getKafkaTimeoutMs() {
    return this.kafkaTimeoutMs;
  }
  public void setKafkaTimeoutMs(int kafkaTimeoutMs) {
    this.kafkaTimeoutMs = kafkaTimeoutMs;
  }
  public short getMaxReplicationFactor() {
    return this.maxReplicationFactor;
  }
  public void setMaxReplicationFactor(short maxReplicationFactor) {
    this.maxReplicationFactor = maxReplicationFactor;
  }
  public int getMaxPartitions() {
    return this.maxPartitions;
  }
  public void setMaxPartitions(int maxPartitions) {
    this.maxPartitions = maxPartitions;
  }
  public long getMaxRetentionMs() {
    return this.maxRetentionMs;
  }
  public void setMaxRetentionMs(long maxRetentionMs) {
    this.maxRetentionMs = maxRetentionMs;
  }

  @Override
  public String toString() {
    return "AppConfig{" +
      "bootstrapServers='" + bootstrapServers + '\'' +
      ", securityProtocol='" + securityProtocol + '\'' +
      ", defaultReplicationFactor=" + defaultReplicationFactor +
      ", enableTopicDelete=" + enableTopicDelete +
      ", enableTopicImport=" + enableTopicImport +
      ", enableAclManagement=" + enableAclManagement +
      ", operatorId='" + operatorId + '\'' +
      ", standardLabels=" + standardLabels +
      ", usernamePoolSecretName='" + usernamePoolSecretName + '\'' +
      ", consumedUsersSecretName='" + consumedUsersSecretName + '\'' +
      ", standardAclLabels=" + standardAclLabels +
      ", metricsPort=" + metricsPort +
      ", healthsPort=" + healthsPort +
      ", kafkaTimeoutMs=" + kafkaTimeoutMs +
      ", maxReplicationFactor=" + maxReplicationFactor +
      ", maxPartitions=" + maxPartitions +
      ", maxRetentionMs=" + maxRetentionMs +
      '}';
  }
}
