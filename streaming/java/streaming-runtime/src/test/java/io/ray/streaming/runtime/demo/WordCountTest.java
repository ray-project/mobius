package io.ray.streaming.runtime.demo;

import com.google.common.collect.ImmutableMap;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.runtime.BaseTest;
import io.ray.streaming.util.Config;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class WordCountTest extends BaseTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(WordCountTest.class);

  static Map<String, Integer> wordCount = new ConcurrentHashMap<>();
  static Map<String, Integer> userDefinedWordCount = new ConcurrentHashMap<>();

  @Test(timeOut = 60000)
  public void testWordCount() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    Map<String, String> config = new HashMap<>();
    config.put(Config.CHANNEL_TYPE, "MEMORY_CHANNEL");
    streamingContext.withConfig(config);
    List<String> text = new ArrayList<>();
    text.add("hello world eagle eagle eagle");
    DataStreamSource<String> streamSource = DataStreamSource.fromCollection(streamingContext, text);
    streamSource
        .flatMap(
            (FlatMapFunction<String, WordAndCount>)
                (value, collector) -> {
                  String[] records = value.split(" ");
                  for (String record : records) {
                    collector.collect(new WordAndCount(record, 1));
                  }
                })
        .filter(pair -> !pair.word.contains("world"))
        .keyBy(pair -> pair.word)
        .reduce(
            (ReduceFunction<WordAndCount>)
                (oldValue, newValue) ->
                    new WordAndCount(oldValue.word, oldValue.count + newValue.count))
        .sink((SinkFunction<WordAndCount>) result -> wordCount.put(result.word, result.count));

    streamingContext.execute("testWordCount");

    ImmutableMap<String, Integer> expected = ImmutableMap.of("eagle", 3, "hello", 1);
    while (!wordCount.equals(expected)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Got an exception while sleeping.", e);
      }
    }
    streamingContext.stop();
  }

  @Test(timeOut = 60000)
  public void testUserDefinedSourceWordCount() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    Map<String, String> config = new HashMap<>();
    config.put(Config.CHANNEL_TYPE, "MEMORY_CHANNEL");
    streamingContext.withConfig(config);
    int totalNum = 100000;
    DataStreamSource.fromSource(streamingContext, new MySourceFunction(totalNum))
        .flatMap(
            (FlatMapFunction<String, WordAndCount>)
                (value, collector) -> {
                  String[] records = value.split(" ");
                  for (String record : records) {
                    collector.collect(new WordAndCount(record, 1));
                  }
                })
        .keyBy(pair -> pair.word)
        .reduce(
            (ReduceFunction<WordAndCount>)
                (oldValue, newValue) ->
                    new WordAndCount(oldValue.word, oldValue.count + newValue.count))
        .sink(
            (SinkFunction<WordAndCount>)
                result -> userDefinedWordCount.put(result.word, result.count));

    streamingContext.execute(jobName);

    while (true) {
      int totalWords = userDefinedWordCount.values().stream().mapToInt(Integer::intValue).sum();
      if (totalWords >= totalNum) {
        LOG.info("Total word size : {}.", totalWords);
        for (Map.Entry<String, Integer> entry : userDefinedWordCount.entrySet()) {
          LOG.info("Word key : {}, count : {}.", entry.getKey(), entry.getValue());
        }
        break;
      }
      try {
        LOG.info("Total word size : {}.", totalWords);
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Got an exception while sleeping.", e);
      }
    }
    streamingContext.stop();
  }

  private static class WordAndCount implements Serializable {

    public final String word;
    public final Integer count;

    public WordAndCount(String key, Integer count) {
      this.word = key;
      this.count = count;
    }
  }

  private class MySourceFunction implements SourceFunction<String> {

    private Random random;
    private int totalNum;
    private int count;

    public MySourceFunction(int totalNum) {
      random = new Random();
      this.totalNum = totalNum;
      this.count = 0;
    }

    @Override
    public void init(int parallelism, int index) {}

    @Override
    public void fetch(SourceContext<String> ctx) throws Exception {
      if (count < totalNum) {
        ctx.collect(String.valueOf(Math.abs(random.nextInt()) % 1024));
        count++;
      } else {
      }
    }

    @Override
    public void close() {}
  }
}
