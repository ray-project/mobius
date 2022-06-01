package io.ray.streaming.runtime.util;

public class GraphBuilder {

  private final String LINE_SEPARATOR = System.getProperty("line.separator");
  private StringBuilder dagBuilder = new StringBuilder();

  public GraphBuilder() {
    dagBuilder.append("digraph G {").append(LINE_SEPARATOR);
  }

  public void append(String from, String to, String connection) {
    dagBuilder.append(from).append(" -> ").append(to).append(" [label =\" ").append(connection)
        .append("\"];").append(LINE_SEPARATOR);
  }

  public void append(String isolatedNode) {
    dagBuilder.append(isolatedNode).append(";").append(LINE_SEPARATOR);
  }

  public String build() {
    dagBuilder.append("}").append(LINE_SEPARATOR);
    return dagBuilder.toString();
  }
}
