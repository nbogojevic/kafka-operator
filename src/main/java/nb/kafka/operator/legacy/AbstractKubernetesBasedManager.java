package nb.kafka.operator.legacy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import nb.kafka.operator.KafkaOperator;
import nb.kafka.operator.Topic;
import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.util.TopicUtil;

@Deprecated
public abstract class AbstractKubernetesBasedManager<T extends HasMetadata>
    implements TopicResourceManager, Watcher<T> {
  private static final Logger log = LoggerFactory.getLogger(AbstractKubernetesBasedManager.class);

  public static final String GENERATED_ANNOTATION = PropertyUtil.kubeAnnotation("generated");
  public static final String GENERATOR_LABEL = "generator";
  public static final String KAFKA_OPERATOR_GENERATOR = "kafka-operator";

  protected Watch watch;
  protected final KafkaOperator operator;
  private final Map<String, String> identifyingLabels;
  private final Class<T> resourceClass;

  public AbstractKubernetesBasedManager(Class<T> resourceClass, KafkaOperator operator, Map<String, String> labels) {
    super();
    this.resourceClass = resourceClass;
    this.operator = operator;
    identifyingLabels = Collections.unmodifiableMap(labels);
  }

  @Override
  public void close() {
    if (watch != null) {
      watch.close();
    }
  }

  protected Map<String, String> labels() {
    return new HashMap<>(identifyingLabels);
  }

  protected String cleanName(String name) {
    return name.replace('_', '-').toLowerCase();
  }

  protected DefaultKubernetesClient kubeClient() {
    return operator.kubeClient();
  }

  protected abstract Topic topicBuilder(T resource);

  @Override
  public void onClose(KubernetesClientException cause) {
    if (cause != null) {
      log.error("Exception while closing {} watch", resourceKind(), cause);
    } else {
      log.info("Closed {} watch", resourceKind());
    }
  }

  @Override
  public void eventReceived(Action action, T resource) {
    if (resource != null) {
      log.info("Got event {} for {} {}", action, resourceKind(), resource.getMetadata().getName());
      if (TopicUtil.isValidTopicName(resource.getMetadata().getName())) {
        switch (action) {
          case ADDED:
            operator.createTopic(topicBuilder(resource));
            break;
          case MODIFIED:
            operator.updateTopic(topicBuilder(resource));
            break;
          case DELETED:
            operator.deleteTopic(topicBuilder(resource).getName());
            break;
          case ERROR:
            log.error("Error event received for {}: {}", resourceKind(), resource);
            break;
        }
      } else {
        String topicName = resource.getMetadata().getName();
        log.warn("{} change {} for protected topic {} was ignored", resourceKind(), action, topicName);
      }
    } else {
      log.warn("Event {} received for null {}", action, resourceKind());
    }
  }

  protected String resourceKind() {
    return resourceClass.getSimpleName();
  }
}
