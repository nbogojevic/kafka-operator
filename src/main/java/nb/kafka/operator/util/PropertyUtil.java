package nb.kafka.operator.util;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PropertyUtil {
  private static final Logger log = LoggerFactory.getLogger(PropertyUtil.class);

  private PropertyUtil() {
  }

  public static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }

  public static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
  }

  public static boolean isNotNullOrEmpty(String str) {
    return !isNullOrEmpty(str);
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName, String envVarName, String defaultValue) {
    String answer = System.getProperty(systemPropertyName);
    if (isNotNullOrEmpty(answer)) {
      return answer;
    }

    answer = System.getenv(envVarName);
    if (isNotNullOrEmpty(answer)) {
      return answer;
    }

    return defaultValue;
  }

  public static String convertSystemPropertyNameToEnvVar(String systemPropertyName) {
    return systemPropertyName.toUpperCase().replaceAll("[.-]", "_");
  }

  public static String getEnvVar(String envVarName, String defaultValue) {
    String answer = System.getenv(envVarName);
    return isNotNullOrEmpty(answer) ? answer : defaultValue;
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName, String defaultValue) {
    return getSystemPropertyOrEnvVar(systemPropertyName, convertSystemPropertyNameToEnvVar(systemPropertyName),
        defaultValue);
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName) {
    return getSystemPropertyOrEnvVar(systemPropertyName, (String)null);
  }

  public static String kubeAnnotation(String path) {
    return "topic.kafka.nb/" + path;
  }

  public static Boolean getSystemPropertyOrEnvVar(String systemPropertyName, Boolean defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, String.valueOf(defaultValue));
    return Boolean.parseBoolean(result);
  }

  public static int getSystemPropertyOrEnvVar(String systemPropertyName, int defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, String.valueOf(defaultValue));
    return Integer.parseInt(result);
  }

  public static long getSystemPropertyOrEnvVar(String systemPropertyName, long defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, String.valueOf(defaultValue));
    return Long.parseLong(result);
  }

  public static short getSystemPropertyOrEnvVar(String systemPropertyName, short defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, String.valueOf(defaultValue));
    return Short.parseShort(result);
  }

  public static String getProperty(Map<String, String> map, String property, String defaultValue) {
    if (map == null) {
      return defaultValue;
    }
    return map.getOrDefault(property, defaultValue);
  }

  public static int getProperty(Map<String, String> map, String property, int defaultValue) {
    String result = getProperty(map, property, String.valueOf(defaultValue));
    return Integer.parseInt(result);
  }

  public static short getProperty(Map<String, String> map, String property, short defaultValue) {
    String result = getProperty(map, property, String.valueOf(defaultValue));
    return Short.parseShort(result);
  }

  public static Boolean getProperty(Map<String, String> map, String property, Boolean defaultValue) {
    String result = getProperty(map, property, String.valueOf(defaultValue));
    return Boolean.parseBoolean(result);
  }

  public static String propertiesAsString(Map<String, String> map) {
    try {
      Properties props = new Properties();
      props.putAll(map);
      StringWriter sw = new StringWriter();
      props.store(sw, null);
      return sw.toString();
    } catch (IOException e) {
      log.error("This exception should not occur.", e);
    }
    return "";
  }

  public static Map<String, String> propertiesFromString(String properties) throws IOException {
    Properties props = new Properties();
    props.load(new StringReader(properties));
    return props.entrySet()
        .stream()
        .collect(Collectors.toMap(e -> (String)e.getKey(), e -> (String)e.getValue()));
  }

  public static Map<String, String> stringToMap(String labels) {
    return Arrays.asList(labels.split(","))
        .stream()
        .map(s -> s.split("="))
        .filter(s -> s.length == 2)
        .collect(Collectors.toMap(s -> s[0], s -> s[1]));
  }
}
