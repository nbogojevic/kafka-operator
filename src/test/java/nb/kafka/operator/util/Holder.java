package nb.kafka.operator.util;

public class Holder<T> {
  private T holded;

  public Holder() {
  }
  
  public Holder(T holded) {
    this.holded = holded;
  }

  public T get() {
    return holded;
  }

  public void set(T holded) {
    this.holded = holded;
  }
}
