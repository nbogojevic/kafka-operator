package nb.kafka.operator;

public class TopicCreationException extends Exception {
  private static final long serialVersionUID = -2135941761570261774L;

  public TopicCreationException() {
    super();
  }

  public TopicCreationException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public TopicCreationException(String message, Throwable cause) {
    super(message, cause);
  }

  public TopicCreationException(String message) {
    super(message);
  }

  public TopicCreationException(Throwable cause) {
    super(cause);
  }
}
