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

public class KubernetesUtilTest {

  @Test
  public void testMakeResourceNameWithUnderscoreInMiddle() {
    //Arrange
    String topicName = "test_topic";
    String expectedResult = "test-topic";

    //Act
    String result = KubernetesUtil.makeResourceName(topicName);

    //Assert
    assertEquals(expectedResult, result);
  }

  @Test
  public void testMakeResourceNameWithUnderscoreInBeginningEnd() {
    //Arrange
    String topicName = "_test@topic_##_";
    String expectedResult = "test-topic";

    //Act
    String result = KubernetesUtil.makeResourceName(topicName);

    //Assert
    assertEquals(expectedResult, result);
  }

  @Test
  public void testMakeResourceNameWithSpecialCharAndDot() {
    //Arrange
    String topicName = "_test.tOPic_##_";
    String expectedResult = "test.topic";

    //Act
    String result = KubernetesUtil.makeResourceName(topicName);

    //Assert
    assertEquals(expectedResult, result);
  }

}