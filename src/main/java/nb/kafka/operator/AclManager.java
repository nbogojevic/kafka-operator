package nb.kafka.operator;

import static nb.common.App.metrics;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Initializer;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import nb.common.Config;

public class AclManager implements Closeable {
  private static final String CONSUMES_TOPICS_ANNOTATION = Config.kubeAnnotation("consumes-topics");
  private static final String PRODUCES_TOPICS_ANNOTATION = Config.kubeAnnotation("produces-topics");
  private static final Object TOPIC_SECRET_NAME = Config.kubeAnnotation("topic-secret");
  private final static Logger log = LoggerFactory.getLogger(AclManager.class);
  private String usernamePoolSecretName;
  private String consumedUsersSecretName;
  private final KafkaOperator operator;
  private final boolean useInitializers;
  private int userPoolAvailable;
  private Watch watch;

  public AclManager(KafkaOperator operator, String usernamePoolSecretName, String consumedUsersSecretName, Map<String, String> map) {
    super();
    this.useInitializers = false;
    this.operator = operator;
    this.userPoolAvailable = 100;
    this.usernamePoolSecretName = usernamePoolSecretName;
    this.consumedUsersSecretName = consumedUsersSecretName;
    metrics().register(MetricRegistry.name("username-pool"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
          return userPoolAvailable;
      }
    });
  }

  public Secret createSecret(HasMetadata owner, Map<String, String> values) {
    Secret secret = new SecretBuilder().withNewMetadata()
        .withName(secretName(owner))
        .endMetadata()
        .withStringData(values)
        .build();
    setOwnership(secret, owner);
    return kubeClient().secrets().create(secret);
  }
  
  private void setOwnership(HasMetadata owned, HasMetadata ...owners) {
    ArrayList<OwnerReference> ownerReferences = new ArrayList<>(owners.length);
    for (HasMetadata owner: owners) {
      ownerReferences.add(new OwnerReferenceBuilder()
        .withApiVersion(owner.getApiVersion())
        .withKind(owner.getKind())
        .withBlockOwnerDeletion(false)
        .withName(owner.getMetadata().getName())
        .withUid(owner.getMetadata().getUid())
        .build());
    }
    owned.getMetadata().setOwnerReferences(ownerReferences);
  }

  public void onNeedSecret(HasMetadata deployment) {
    if (useInitializers) {
      if (deployment.getMetadata().getInitializers() == null) {
        return;
      }
      List<Initializer> pending = deployment.getMetadata().getInitializers().getPending();
      if (pending == null || pending.isEmpty() || !"kafka-operator".equals(pending.get(0).getName())) {
        return;
      }
      log.info("Initializing deployment: {}", deployment.getMetadata().getName());        
    }
    Collection<String> consumedTopics = asList(deployment, CONSUMES_TOPICS_ANNOTATION);
    Collection<String> producedTopics = asList(deployment, PRODUCES_TOPICS_ANNOTATION);
    String secretName = deployment.getMetadata().getAnnotations().get(TOPIC_SECRET_NAME);
    if (secretName == null) {      
      secretName = deployment.getMetadata().getName() + "-kafka-credentials";
    }
    Secret exists = kubeClient().secrets().withName(secretName).get();
    if (exists != null) {
      log.debug("Secret with kafka credentials already exists for {}", deployment.getMetadata().getName());
      return;
    }
    Map.Entry<String, String> assignedUser = allocateUser(deployment);
    operator.kafkaUtils().setUpAclForUser(assignedUser.getKey(), consumedTopics, producedTopics);
    Map<String, String> secretMap = new HashMap<>();
    secretMap.put("username", assignedUser.getKey());
    secretMap.put("password", assignedUser.getValue());
    secretMap.put("bootstrap.server", operator.getBootstrapServer());
    secretMap.put("kafka-client-jaas.conf", jaasConf(assignedUser.getKey(), assignedUser.getValue()));
    
    Map<String, String> labels = new HashMap<>();
    labels.put(AbstractKubernetesBasedManager.GENERATOR_LABEL, operator.getGeneratorId());
    labels.put("config", "kafka-topic-credentials");
    Map<String, String> annotations = new HashMap<>();
    annotations.put(AbstractKubernetesBasedManager.GENERATED_ANNOTATION, ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
    Secret topicSecret = new SecretBuilder().withNewMetadata()
      .withName(secretName)
      .withLabels(labels)
      .withAnnotations(annotations)
      .endMetadata()
      .withStringData(secretMap)
      .build();
    // Should we make it owned by all topics too?
    setOwnership(topicSecret, deployment);
    kubeClient().secrets().create(topicSecret);
    if (useInitializers) {
      List<Initializer> pending = new ArrayList<>(deployment.getMetadata().getInitializers().getPending());
      pending.remove(0);
      deployment.getMetadata().getInitializers().setPending(pending);
      update(deployment);
    }
  }

  protected void update(HasMetadata deployment) {
    kubeClient().extensions().deployments().createOrReplace((Deployment) deployment);      
  }

  private String jaasConf(String username, String password) {
    StringBuilder sb = new StringBuilder();
    sb.append("KafkaClient {\n")
      .append(" org.apache.kafka.common.security.plain.PlainLoginModule required\n")
      .append(" username=").append(username).append('\n')
      .append(" password=").append(password).append(";\n")
      .append("};");
    return sb.toString();
  }

  private Map.Entry<String, String> allocateUser(HasMetadata deployment) {
    Secret usernamePoolSecret = kubeClient().secrets().withName(usernamePoolSecretName).get();
    Secret consumedUsersSecret = kubeClient().secrets().withName(consumedUsersSecretName).get();
    Map<String, String> usernamePool = decodeMap(usernamePoolSecret.getData().get("username-pool"));
    Collection<String> consumedUsernames = decodeList(consumedUsersSecret.getData().get("consumed-usernames"));
    // How much of the pool is used
    userPoolAvailable = ((usernamePool.size() - consumedUsernames.size()) * 100) / usernamePool.size();
    // Remove all consumed usernames
    consumedUsernames.forEach(k -> usernamePool.remove(k));
    if (usernamePool.isEmpty()) {
      throw new IllegalStateException("Username pool is exhausted. Please check Secret " + usernamePoolSecretName + " and " + consumedUsersSecretName);
    }
    // Take first element
    Map.Entry<String, String> pair = usernamePool.entrySet().iterator().next();
    consumedUsernames.add(pair.getKey());
    Secret updatedSecret = new SecretBuilder()
        .withNewMetadata()
          .withAnnotations(consumedUsersSecret.getMetadata().getAnnotations())
          .withLabels(consumedUsersSecret.getMetadata().getLabels())
          .withName(consumedUsersSecret.getMetadata().getName())
          .endMetadata()
        .withData(null)
        .withStringData(Collections.singletonMap("consumed-usernames", consumedUsernames.stream().collect(Collectors.joining("\n")))).build();
    // TODO handle failure case
    kubeClient().secrets().createOrReplace(updatedSecret);
    return pair;
  }

  private Collection<String> decodeList(String string) {
    return Arrays.asList(string.split("\n")).stream().filter(s -> !s.trim().isEmpty()).collect(Collectors.toCollection(ArrayList::new));
  }

  private Map<String, String> decodeMap(String string) {
    string = new String(Base64.getDecoder().decode(string), StandardCharsets.UTF_8);
    return Arrays.asList(string.split("\n")).stream().map(s -> s.split("="))
      .filter(s -> s.length == 2).collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
  }

  private Collection<String> asList(HasMetadata deployment, String topics) {
    String value = deployment.getMetadata().getAnnotations().get(topics);
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptySet();
    }
    // Return non empty strings as set
    return Arrays.asList(value.split(",")).stream().map(String::trim).filter(String::isEmpty).collect(Collectors.toSet());
  }

  public Secret hasSecret(HasMetadata owner) {
    return kubeClient().secrets().withName(secretName(owner)).get();
  }
  
  private String secretName(HasMetadata owner) {
    return owner.getMetadata().getName();
  }
  
  public void watch() {
    watch = kubeClient().extensions().deployments().withLabel("kafka-operator", "inject-credentials").watch(new DeploymentWatcher());
    log.info("Watching Deployments for credential requests.");
  }

  public void onRemoved(HasMetadata deployment) {
    String secretName = deployment.getMetadata().getAnnotations().get(TOPIC_SECRET_NAME);
    if (secretName == null) {      
      secretName = deployment.getMetadata().getName()+"-kafka-credentials";
    }
    kubeClient().secrets().withName(secretName).delete();
  }
  
  class DeploymentWatcher implements Watcher<Deployment> {
    @Override
    public void eventReceived(Action action, Deployment resource) {
      if (resource != null) {
        log.info("Got event {} for {} {}", action, resourceKind(), resource.getMetadata().getName());
        switch (action) {
          case ADDED:
          case MODIFIED:
            onNeedSecret(resource);
            break;
          case DELETED:
            onRemoved(resource);
            break;
          case ERROR:
            log.error("Error event received for {}: {}", resourceKind(), resource);
            break;
        }
      } else {
        log.warn("Event {} received for null {}", action, resourceKind());
      }
    }
  
    private String resourceKind() {
      return "Deployment";
    }

    @Override
    public void onClose(KubernetesClientException cause) {
      if (cause != null) {
        log.error("Exception while closing {} watch", resourceKind(), cause);
      } else {
        log.info("Closed {} watch", resourceKind(), cause);
      }
    }
  }

  public void close() {
    if (watch != null) {
      watch.close();
    }
  }

  protected DefaultKubernetesClient kubeClient() {
    return operator.kubeClient();
  }
}
