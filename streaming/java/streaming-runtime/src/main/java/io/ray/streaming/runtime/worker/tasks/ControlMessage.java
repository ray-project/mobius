package io.ray.streaming.runtime.worker.tasks;

import com.google.common.base.MoreObjects;
import java.io.Serializable;

public class ControlMessage<T> implements Serializable {

  private final T message;
  private final MessageType messageType;
  private final Object extra;

  public ControlMessage(T message, MessageType messageType, Object extra) {
    this.message = message;
    this.messageType = messageType;
    this.extra = extra;
  }

  public ControlMessage(T message, MessageType messageType) {
    this(message, messageType, null);
  }

  public T getMessage() {
    return message;
  }

  public MessageType getMessageType() {
    return messageType;
  }

  public Object getExtra() {
    return extra;
  }

  public enum MessageType {
    UPDATE_CONTEXT(0, "update context"),
    BROADCAST_PARTIAL_BARRIER(1, "broadcast partial barrier"),
    CLEAR_PARTIAL_BARRIER(2, "clear partial barrier"),
    REINITIALIZE_OPERATOR(3, "re-init alive operator"),
    EXCEPTION_INJECTION(4, "exception injection"),
    OPERATOR_COMMAND(5, "forward command");

    private int value;
    private String desc;

    MessageType(int value, String desc) {
      this.value = value;
      this.desc = desc;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", messageType)
        .add("message", message)
        .add("extra", extra)
        .toString();
  }
}
