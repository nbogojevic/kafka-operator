package nb.kafka.operator.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class WhiteboxUtil {
  private WhiteboxUtil() {
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

  public static <T, R> R runMethod(T object, String methodName, Class<?>[] argTypes, Object... args) throws Throwable {
    return runMethod(object.getClass(), methodName, argTypes, object, args);
  }
  
  @SuppressWarnings("unchecked")
  public static <T, R> R runMethod(Class<?> clazz, String methodName, Class<?>[] argTypes, T object, Object... args) throws Throwable {
    Method method = clazz.getDeclaredMethod(methodName, argTypes);
    method.setAccessible(true);
    try {
      return (R) method.invoke(object, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() != null) {
        throw e.getCause();
      }
      throw e;
    }
  }
  
  public static <T, R> R runMethod(Class<?> clazz, String methodName, Class<?> argType, T object, Object... args) throws Throwable {
    return runMethod(clazz, methodName, new Class<?>[] { argType }, object, args);
  }
}
