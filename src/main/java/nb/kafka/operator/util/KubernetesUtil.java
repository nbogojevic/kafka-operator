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
    return name.replace('_', '-').toLowerCase();
  }

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
