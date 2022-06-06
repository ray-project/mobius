package io.ray.streaming.python;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.Language;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.Operator;
import io.ray.streaming.operator.OperatorType;
import io.ray.streaming.operator.StreamOperator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/** Represents a {@link StreamOperator} that wraps python {@link PythonFunction}. */
@SuppressWarnings("unchecked")
public class PythonOperator extends AbstractStreamOperator {

  private final String moduleName;
  private final String className;

  public PythonOperator(String moduleName, String className) {
    super(null);
    this.moduleName = moduleName;
    this.className = className;
  }

  public PythonOperator(PythonFunction function) {
    super(function);
    this.moduleName = null;
    this.className = null;
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public void open(List list, RuntimeContext runtimeContext) {
    throwUnsupportedException();
  }

  @Override
  public void close() {
    throwUnsupportedException();
  }

  void throwUnsupportedException() {
    StackTraceElement[] trace = Thread.currentThread().getStackTrace();
    Preconditions.checkState(trace.length >= 2);
    StackTraceElement traceElement = trace[2];
    String msg =
        String.format(
            "Method %s.%s shouldn't be called.",
            traceElement.getClassName(), traceElement.getMethodName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public OperatorInputType getOpType() {
    PythonFunction pythonFunction = getFunction();
    if (pythonFunction
        .getFunctionInterface()
        .equals(PythonFunction.FunctionInterface.SOURCE_FUNCTION)) {
      return OperatorInputType.SOURCE;
    } else {
      return OperatorInputType.ONE_INPUT;
    }
  }

  @Override
  public PythonFunction getFunction() {
    return (PythonFunction) super.getFunction();
  }

  @Override
  public String getName() {
    StringBuilder builder = new StringBuilder();
    builder.append(PythonOperator.class.getSimpleName()).append("[");
    if (function != null) {
      builder.append(((PythonFunction) function).toSimpleString());
    } else {
      builder.append(moduleName).append(".").append(className);
    }
    return builder.append("]").toString();
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner =
        new StringJoiner(", ", PythonOperator.class.getSimpleName() + "[", "]");
    if (function != null) {
      stringJoiner.add("function='" + function + "'");
    } else {
      stringJoiner.add("moduleName='" + moduleName + "'").add("className='" + className + "'");
    }
    return stringJoiner.toString();
  }

  public static class ChainedPythonOperator extends PythonOperator {

    private final List<PythonOperator> operators;
    private final PythonOperator headOperator;
    private final PythonOperator tailOperator;
    private final List<Map<String, String>> configs;

    public ChainedPythonOperator(
        List<PythonOperator> operators, List<Map<String, String>> configs) {
      super(null);
      Preconditions.checkArgument(!operators.isEmpty());
      this.operators = operators;
      this.configs = configs;
      this.headOperator = operators.get(0);
      this.tailOperator = operators.get(operators.size() - 1);
    }

    @Override
    public OperatorInputType getOpType() {
      return headOperator.getOpType();
    }

    @Override
    public Language getLanguage() {
      return Language.PYTHON;
    }

    @Override
    public String getName() {
      if (!getClass().getSimpleName().equals(name)) {
        return "PythonOperator_" + getModuleName() + "." + getClassName();
      }
      return name;
    }

    @Override
    public String getModuleName() {
      throwUnsupportedException();
      return null; // impossible
    }

    @Override
    public String getClassName() {
      throwUnsupportedException();
      return null; // impossible
    }

    public List<PythonOperator> getOperators() {
      return operators;
    }

    public PythonOperator getHeadOperator() {
      return headOperator;
    }

    public PythonOperator getTailOperator() {
      return tailOperator;
    }

    public List<Map<String, String>> getConfigs() {
      return configs;
    }
  }
}
