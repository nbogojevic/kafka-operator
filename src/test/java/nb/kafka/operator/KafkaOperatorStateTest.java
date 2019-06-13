package nb.kafka.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.KubernetesClient;
import nb.kafka.operator.KafkaOperator.State;
import nb.kafka.operator.util.MeterManager;
import nb.kafka.operator.watch.ConfigMapWatcher;

public class KafkaOperatorStateTest {
  private AppConfig config = AppConfig.defaultConfig();

  @BeforeEach
  void setUp() {
    config.setKafkaUrl("localhost:9092");
  }

  @Test
  public void regularStateTest() throws Throwable {
    KubernetesClient kubeClientMock = mock(KubernetesClient.class);
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    ConfigMapWatcher watcherMock = mock(ConfigMapWatcher.class);
    when(watcherMock.listTopics()).thenReturn(Arrays.asList(new Topic("test-topic", 1, (short)1, null, false)));

    KafkaOperator operator = new KafkaOperator(config, kubeClientMock, kafkaAdminMock, watcherMock,
        MeterManager.defaultMeterManager());

    assertEquals(State.CREATED, operator.getState());

    operator.watch();

    assertEquals(State.RUNNING, operator.getState());

    operator.shutdown();

    assertEquals(State.STOPPED, operator.getState());
  }

  @Test
  public void failureStateTest() throws Throwable {
    KubernetesClient kubeClientMock = mock(KubernetesClient.class);
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    ConfigMapWatcher watcherMock = mock(ConfigMapWatcher.class);
    when(watcherMock.listTopics()).thenReturn(Arrays.asList(new Topic("test-topic", 1, (short)1, null, false)));
    doThrow(RuntimeException.class).when(watcherMock).watch();

    KafkaOperator operator = new KafkaOperator(config, kubeClientMock, kafkaAdminMock, watcherMock,
        MeterManager.defaultMeterManager());

    assertEquals(State.CREATED, operator.getState());

    try {
      operator.watch();
    } catch (RuntimeException e) {
    }

    assertEquals(State.FAILED, operator.getState());
  }
}
