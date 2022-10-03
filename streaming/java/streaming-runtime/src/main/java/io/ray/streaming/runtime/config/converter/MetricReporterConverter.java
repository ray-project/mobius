package io.ray.streaming.runtime.config.converter;

import io.ray.streaming.common.utils.TestHelper;
import io.ray.streaming.runtime.config.global.MetricConfig;
import java.lang.reflect.Method;
import org.aeonbits.owner.Converter;

/** MetricReporterConverter */
public class MetricReporterConverter implements Converter<String> {

  /**
   * Converts the given input into an Object of type T. If the method returns null, null will be
   * returned by the Config object. The converter is instantiated for every call, so it shouldn't
   * have any internal state.
   */
  @Override
  public String convert(Method method, String input) {
    if (TestHelper.isUTPattern() && MetricConfig.METRICS_REPORTERS_DEFAULT_VALUE.equals(input)) {
      return "";
    }
    return input;
  }
}
