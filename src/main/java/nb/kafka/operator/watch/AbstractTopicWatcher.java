package nb.kafka.operator.watch;

import java.util.function.Consumer;

import nb.kafka.operator.Topic;

/**
 * Abstract implementation of topic model watcher, providing simple listener management.
 */
public abstract class AbstractTopicWatcher implements TopicWatcher {
  private Consumer<Topic> onCreate;
  private Consumer<Topic> onUpdate;
  private Consumer<String> onDelete;

  @Override
  public void setOnCreateListener(Consumer<Topic> onCreate) {
    this.onCreate = onCreate;
  }

  @Override
  public void setOnUpdateListener(Consumer<Topic> onUpdate) {
    this.onUpdate = onUpdate;
  }

  @Override
  public void setOnDeleteListener(Consumer<String> onDelete) {
    this.onDelete = onDelete;
  }

  protected void emitCreate(Topic topic) {
    if (onCreate != null) {
      onCreate.accept(topic);
    }
  }

  protected void emitUpdate(Topic topic) {
    if (onUpdate != null) {
      onUpdate.accept(topic);
    }
  }

  protected void emitDelete(String topic) {
    if (onDelete != null) {
      onDelete.accept(topic);
    }
  }
}
