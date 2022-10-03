package io.ray.streaming.runtime.config.converter;

import io.ray.streaming.common.utils.TestHelper;
import java.lang.reflect.Method;
import org.aeonbits.owner.Converter;

public class MetricEnableConverter implements Converter<Boolean> {

  @Override
  public Boolean convert(Method method, String value) {
    if (TestHelper.isUTPattern()) {
      return false;
    }
    return Boolean.parseBoolean(value);
  }
}
