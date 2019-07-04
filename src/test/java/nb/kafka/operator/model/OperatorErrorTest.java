package nb.kafka.operator.model;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

public class OperatorErrorTest {
  @Test
  public void testToString() {
    //Arrange
    //Act
    String message = OperatorError.EXCEEDS_MAX_REPLICATION_FACTOR.toString();
    System.out.println(message);

    //Assert
    assertNotNull(message);
  }
}