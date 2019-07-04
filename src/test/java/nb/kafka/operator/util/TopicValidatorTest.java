package nb.kafka.operator.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nb.kafka.operator.AppConfig;
import nb.kafka.operator.PartitionedTopic;
import nb.kafka.operator.Topic;
import nb.kafka.operator.model.OperatorError;

public class TopicValidatorTest {
  private AppConfig appConfig;

  @BeforeEach
  public void setUp() {
    this.appConfig = AppConfig.defaultConfig();
  }

  @Test
  public void testValidateTopicNameNotValid() {
    //Arrange
    Topic topic = new Topic("__topic-name", 10, (short)1, null, false);
    TopicValidator topicValidator = new TopicValidator(null, topic);
    int expectedErrorCode = OperatorError.NOT_VALID_TOPIC_NAME.getCode();

    //Act
    OperatorError operatorError = topicValidator.validateTopicName();

    //Assert
    assertEquals(expectedErrorCode, operatorError.getCode());
  }

  @Test
  public void testValidateTopicNameValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 10, (short)1, null, false);
    TopicValidator topicValidator = new TopicValidator(null, topic);

    //Act
    OperatorError operatorError = topicValidator.validateTopicName();

    //Assert
    assertNull(operatorError);
  }

  @Test
  public void testValidateReplicationFactorNotValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 10, (short)4, null, false);
    appConfig.setMaxReplicationFactor((short) 2);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);
    int expectedErrorcode = OperatorError.EXCEEDS_MAX_REPLICATION_FACTOR.getCode();

    //Act
    OperatorError operatorError = topicValidator.validateReplicationFactor();

    //Assert
    assertEquals(expectedErrorcode, operatorError.getCode());
  }

  @Test
  public void testValidateReplicationFactorValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 10, (short)2, null, false);
    appConfig.setMaxReplicationFactor((short) 3);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);

    //Act
    OperatorError operatorError = topicValidator.validateReplicationFactor();

    //Assert
    assertNull(operatorError);
  }

  @Test
  public void testValidatePartitionsNotValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 20, (short)4, null, false);
    appConfig.setMaxPartitions(10);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);
    int expectedErrorcode = OperatorError.EXCEEDS_MAX_PARTITIONS.getCode();

    //Act
    OperatorError operatorError = topicValidator.validatePartitions();

    //Assert
    assertEquals(expectedErrorcode, operatorError.getCode());
  }

  @Test
  public void testValidatePartitionsValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 10, (short)2, null, false);
    appConfig.setMaxPartitions(20);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);

    //Act
    OperatorError operatorError = topicValidator.validatePartitions();

    //Assert
    assertNull(operatorError);
  }

  @Test
  public void testValidateRetentionMsNotValid() {
    //Arrange
    Map<String, String> properties = new HashMap<>();
    properties.put("retention.ms", "5000001");
    Topic topic = new Topic("topic-name", 20, (short)4, properties, false);
    appConfig.setMaxRetentionMs(5000000);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);
    int expectedErrorCode = OperatorError.EXCEEDS_MAX_RETENTION_MS.getCode();

    //Act
    OperatorError operatorError = topicValidator.validateRetentionMs();

    //Assert
    assertEquals(expectedErrorCode, operatorError.getCode());
  }

  @Test
  public void testValidateRetentionMsValid() {
    //Arrange
    Map<String, String> properties = new HashMap<>();
    properties.put("retention.ms", "5000000");
    Topic topic = new Topic("topic-name", 20, (short)4, properties, false);
    appConfig.setMaxRetentionMs(5000001);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);

    //Act
    OperatorError operatorError = topicValidator.validateRetentionMs();

    //Assert
    assertNull(operatorError);
  }

  @Test
  void testValidatePartitionsChangeValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 30, (short)4, null, false);
    PartitionedTopic existingTopic = new PartitionedTopic("topic-name", 20, (short)4, null, false,
        Collections.emptyList());
    AppConfig appConfig = AppConfig.defaultConfig();
    TopicValidator topicValidator = new TopicValidator(appConfig, topic, existingTopic);

    //Act
    OperatorError operatorError = topicValidator.validatePartitionsChange();

    //Assert
    assertNull(operatorError);
  }

  @Test
  void testValidatePartitionsChangeNotValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 20, (short)4, null, false);
    PartitionedTopic existingTopic = new PartitionedTopic("topic-name", 30,
      (short)4, null, false, Collections.emptyList());
    AppConfig appConfig = AppConfig.defaultConfig();
    TopicValidator topicValidator = new TopicValidator(appConfig, topic, existingTopic);
    int expectedErrorCode = OperatorError.PARTITIONS_REDUCTION_NOT_ALLOWED.getCode();

    //Act
    OperatorError operatorError = topicValidator.validatePartitionsChange();

    //Assert
    assertEquals(expectedErrorCode, operatorError.getCode());
  }

  @Test
  void testvValidateReplicationChangeValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 30, (short)4, null, false);
    PartitionedTopic existingTopic = new PartitionedTopic("topic-name", 20,
      (short)4, null, false, Collections.emptyList());
    AppConfig appConfig = AppConfig.defaultConfig();
    TopicValidator topicValidator = new TopicValidator(appConfig, topic, existingTopic);

    //Act
    OperatorError operatorError = topicValidator.validateReplicationChange();

    //Assert
    assertNull(operatorError);
  }

  @Test
  void testValidateReplicationChangeNotValid() {
    //Arrange
    Topic topic = new Topic("topic-name", 20, (short)4, null, false);
    PartitionedTopic existingTopic = new PartitionedTopic("topic-name", 30,
      (short)3, null, false, Collections.emptyList());
    AppConfig appConfig = AppConfig.defaultConfig();
    TopicValidator topicValidator = new TopicValidator(appConfig, topic, existingTopic);
    int expectedErrorCode = OperatorError.REPLICATION_FACTOR_CHANGE_NOT_SUPPORTED.getCode();

    //Act
    OperatorError operatorError = topicValidator.validateReplicationChange();

    //Assert
    assertEquals(expectedErrorCode, operatorError.getCode());
  }


  @Test
  void testValidateAllValid() {
    //Arrange
    Map<String, String> properties = new HashMap<>();
    properties.put("retention.ms", "200000");
    Topic topic = new Topic("topic-name", 20, (short)2, properties, false);
    appConfig.setMaxRetentionMs(5000000);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);

    //Act
    Set<OperatorError> errors = topicValidator.validate();

    //Assert
    assertTrue(errors.isEmpty());
  }

  @Test
  public void testValidateSomeNotValid() {
    //Arrange
    Map<String, String> properties = new HashMap<>();
    properties.put("retention.ms", "20000000");
    Topic topic = new Topic("topic-name", 20, (short)3, properties, false);
    appConfig.setMaxRetentionMs(5000000);
    appConfig.setMaxReplicationFactor((short)2);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);

    //Act
    Set<OperatorError> errors = topicValidator.validate();

    //Assert
    assertFalse(errors.isEmpty());
    assertEquals(2, errors.size());
  }

  @Test
  public void testIsValidAllValid() {
    //Arrange
    Map<String, String> properties = new HashMap<>();
    properties.put("retention.ms", "200000");
    Topic topic = new Topic("topic-name", 20, (short)2, properties, false);
    appConfig.setMaxRetentionMs(5000000);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);

    //Act
    Boolean isValid = topicValidator.isValid();

    //Assert
    assertTrue(isValid);
  }

  @Test
  public void testIsValidSomeNotValid() {
    //Arrange
    Map<String, String> properties = new HashMap<>();
    properties.put("retention.ms", "20000000");
    Topic topic = new Topic("topic-name", 20, (short)3, properties, false);
    appConfig.setMaxRetentionMs(5000000);
    appConfig.setMaxReplicationFactor((short)2);
    TopicValidator topicValidator = new TopicValidator(appConfig, topic);

    //Act
    Boolean isValid = topicValidator.isValid();

    //Assert
    assertFalse(isValid);
  }
}