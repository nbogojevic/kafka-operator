package nb.kafka.operator;

import java.util.HashMap;
import java.util.Map;

/**
 * Hold the application configuration.
 */
public class AppConfig {
  private String kafkaUrl;
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

  private static AppConfig defaultConfig;
  public static final AppConfig defaultConfig() {
    if (defaultConfig == null) {
      AppConfig conf = new AppConfig();
      conf.kafkaUrl = "kafka:9092";
      conf.securityProtocol = "";
      conf.defaultReplicationFactor = (short)2;
      conf.enableTopicDelete = false;
      conf.enableTopicImport = true;
      conf.enableAclManagement = false;
      conf.operatorId = "kafka-operator";
      conf.standardLabels = new HashMap<>();
      conf.usernamePoolSecretName = "kafka-cluster-kafka-auth-pool";
      conf.consumedUsersSecretName = "kafka-cluster-kafka-consumed-auth-pool";
      conf.standardAclLabels = new HashMap<>();
      defaultConfig = conf;
    }
    return defaultConfig;
  }
  
  public String getKafkaUrl() {
    return kafkaUrl;
  }
  public void setKafkaUrl(String kafkaUrl) {
    this.kafkaUrl = kafkaUrl;
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
    return operatorId != null ? operatorId : "kafka-operator";
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
  @Override
  public String toString() {
    return "AppConfig [kafkaUrl=" + kafkaUrl + ", securityProtocol=" + securityProtocol + ", defaultReplicationFactor="
        + defaultReplicationFactor + ", enableTopicDelete=" + enableTopicDelete + ", enableTopicImport="
        + enableTopicImport + ", enableAclManagement=" + enableAclManagement + ", operatorId=" + operatorId
        + ", standardLabels=" + standardLabels + ", usernamePoolSecretName=" + usernamePoolSecretName
        + ", consumedUsersSecretName=" + consumedUsersSecretName + ", standardAclLabels=" + standardAclLabels + "]";
  }
}
