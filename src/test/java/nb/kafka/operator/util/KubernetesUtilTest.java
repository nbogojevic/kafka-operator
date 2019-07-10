package nb.kafka.operator.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

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