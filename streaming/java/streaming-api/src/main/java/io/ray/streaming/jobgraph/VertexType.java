package io.ray.streaming.jobgraph;

/** Different roles for a node. */
public enum VertexType {
  source, // data reader, 0 input, 1 output
  process, // 1 input, 1 output
  sink, // data writer, 1 input, 0 output
  union, // simply group all input elements, 2 inputs, 1 output,
  join // group input elements with a specified method, 2 inputs, 1 output
}

