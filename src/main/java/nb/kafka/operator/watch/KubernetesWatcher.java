package nb.kafka.operator.watch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.util.TopicUtil;

/**
 * A skeleton class for watching Kubernetes resources that represent topic models.
 * @param <T> The type of Kubernetes resource to watch.
 */
public abstract class KubernetesWatcher<T extends HasMetadata> extends AbstractTopicWatcher implements Watcher<T> {
  private static final Logger log = LoggerFactory.getLogger(KubernetesWatcher.class);

  private final Map<String, String> identifyingLabels;
  private final Class<T> resourceClass;
  private final KubernetesClient client;
  private final AppConfig config;

  public KubernetesWatcher(KubernetesClient client, Class<T> resourceClass, AppConfig config) {
    this.resourceClass = resourceClass;
    this.client = client;
    this.config = config;
    this.identifyingLabels = Collections.unmodifiableMap(config.getStandardLabels());
  }

  protected abstract Topic buildTopicModel(T resource);

  @Override
  public void eventReceived(Action action, T resource) {
    if (resource == null) {
      log.warn("Event {} received for null resource {}", action, resourceKind());
      return;
    }

    String topicName = resource.getMetadata().getName();
    log.info("Got event {} for {} {}", action, resourceKind(), topicName);

    if (!TopicUtil.isValidTopicName(topicName)) {
      log.warn("{} change {} for protected topic {} was ignored", resourceKind(), action, topicName);
      return;
    }

    switch (action) {
      case ADDED:
        emitCreate(buildTopicModel(resource));
        break;
      case MODIFIED:
        emitUpdate(buildTopicModel(resource));
        break;
      case DELETED:
        emitDelete(buildTopicModel(resource).getName());
        break;
      case ERROR:
        log.error("Error event received for {}: {}", resourceKind(), resource);
        break;
    }
  }

  public String resourceKind() {
    return resourceClass.getSimpleName();
  }

  public Map<String, String> labels() {
    return new HashMap<>(identifyingLabels);
  }

  @Override
  public void onClose(KubernetesClientException cause) {
    if (cause != null) {
      log.error("Exception while closing {} watch", resourceKind(), cause);
    } else {
      log.info("Closed {} watch", resourceKind());
    }
  }

  public KubernetesClient kubeClient() {
    return client;
  }

  public AppConfig appConfig() {
    return config;
  }
}
