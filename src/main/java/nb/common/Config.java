package nb.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class Config {
  Properties internalProperties = new Properties();
  
  private Config() {
  }

  public static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
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
    return getSystemPropertyOrEnvVar(systemPropertyName, (String) null);
  }

  public static Boolean getSystemPropertyOrEnvVar(String systemPropertyName, Boolean defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, String.valueOf(defaultValue));
    return Boolean.parseBoolean(result);
  }

  public static int getSystemPropertyOrEnvVar(String systemPropertyName, int defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, String.valueOf(defaultValue));
    return Integer.parseInt(result);
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

  public static Boolean getProperty(Map<String, String> map, String property, Boolean defaultValue) {
    String result = getProperty(map, property, String.valueOf(defaultValue));
    return Boolean.parseBoolean(result);
  }

  public static Map<String, String> mapFromProperties(Properties props) {
    Map<String, String> propMap = new HashMap<>();
    props.forEach((k, v) -> propMap.put((String) k, (String) v));
    return propMap;
  }

  public static Properties propertiesFromMap(Map<String, String> use) {
    Properties props = new Properties();
    props.putAll(use);
    return props;
  }
}
