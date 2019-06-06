package nb.kafka.operator.util;

import java.lang.reflect.Field;

public final class FieldUtil {
  private FieldUtil() {
  }

  public static <T, F> void injectField(T object, String fieldName, F newValue) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    Field field = object.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(object, newValue);
  }

  @SuppressWarnings("unchecked")
  public static <T, F> F readField(T object, String fieldName) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    Field field = object.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return (F) field.get(object);
  }
}
