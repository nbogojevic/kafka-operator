package nb.kafka.operator.util;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

public final class KubernetesUtil {

  private static final String CUSTOM_RESOURCE_VERSION = "v1alpha";
  private static final String CUSTOM_RESOURCE_GROUP = "nb";
  private static final String KAFKATOPIC_SINGULAR = "kafkatopic";
  private static final String KAFKATOPICS_PLURAL = "kafkatopics";
  private static final String CUSTOM_RESOURCE_DEFINITION_NAME = KAFKATOPICS_PLURAL + "." + CUSTOM_RESOURCE_GROUP;

  private KubernetesUtil() {
  }

  public static String makeResourceName(String name) {
    String resourceName = name.replaceAll("[^-a-zA-Z0-9-.]+","-").toLowerCase();
    if (resourceName.startsWith("-") || resourceName.startsWith(".")) {
      resourceName = resourceName.substring(1);
    }
    if (resourceName.endsWith("-") || resourceName.endsWith(".")) {
      resourceName = removeLastChar(resourceName);
    }
    return resourceName;
  }

  private static String removeLastChar(String str) {
    return str.substring(0, str.length() - 1);
  }

  /**
   * Load or create a {@link CustomResourceDefinition} of the given kind.
   * @param client A Kubernetes client.
   * @param resourceKind The resource kind.
   * @return
   */
  public static CustomResourceDefinition getTopicCrd(KubernetesClient client, String resourceKind) {
    CustomResourceDefinition crd = client.customResourceDefinitions().withName(CUSTOM_RESOURCE_DEFINITION_NAME).get();
    if (crd == null) {
      crd = new CustomResourceDefinitionBuilder()
          .withNewMetadata().withName(CUSTOM_RESOURCE_DEFINITION_NAME).endMetadata()
          .withNewSpec()
          .withGroup(CUSTOM_RESOURCE_GROUP)
          .withVersion(CUSTOM_RESOURCE_VERSION)
          .withScope("Namespaced")
          .withNewNames()
            .withKind(resourceKind).withPlural(KAFKATOPICS_PLURAL)
            .withSingular(KAFKATOPIC_SINGULAR)
            .endNames()
          .endSpec().build();

      crd = client.customResourceDefinitions().create(crd);
    }
    return crd;
  }
}
