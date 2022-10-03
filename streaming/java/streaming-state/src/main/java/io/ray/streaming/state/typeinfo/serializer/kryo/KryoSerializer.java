package io.ray.streaming.state.typeinfo.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataInputViewStream;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.buffer.DataOutputViewStream;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.List;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Use kryo for types the RayState serializer cannot handle.
 *
 * <p>NOTE: Thread unsafe.
 */
public class KryoSerializer<T> extends TypeSerializer<T> {

  static class KryoThreadLocalContext {
    private Kryo kryo;
    private Input input;
    private Output output;
    private DataOutputView previousOut;
    private DataInputView previousInput;

    public KryoThreadLocalContext(Kryo kryo, Input input, Output output) {
      this.kryo = kryo;
      this.input = input;
      this.output = output;
    }

    private Kryo getKryo() {
      return kryo;
    }

    public Input getInput() {
      return input;
    }

    public void setInput(Input input) {
      this.input = input;
    }

    public Output getOutput() {
      return output;
    }

    public void setOutput(Output output) {
      this.output = output;
    }

    public DataOutputView getPreviousOut() {
      return previousOut;
    }

    public void setPreviousOut(DataOutputView previousOut) {
      this.previousOut = previousOut;
    }

    public DataInputView getPreviousInput() {
      return previousInput;
    }

    public void setPreviousInput(DataInputView previousInput) {
      this.previousInput = previousInput;
    }
  }

  private final Class<T> type;
  private final List<KryoRegistration> kryoRegistrationList;
  private final ThreadLocal<KryoThreadLocalContext> kryoThreadLocalContext;

  public KryoSerializer(Class<T> type) {
    this(type, null);
  }

  public KryoSerializer(Class<T> type, List<KryoRegistration> registrationList) {
    this.type = type;
    this.kryoRegistrationList = registrationList;

    kryoThreadLocalContext =
        ThreadLocal.withInitial(
            () -> {
              Kryo kryo = createKryoInstance();
              Input input = new Input();
              Output output = new Output();
              return new KryoThreadLocalContext(kryo, input, output);
            });
  }

  @Override
  public void serialize(T record, DataOutputView outputView) throws IOException {
    KryoThreadLocalContext context = kryoThreadLocalContext.get();
    Output output = null;

    try {
      if (outputView != context.previousOut) {
        DataOutputViewStream outputViewStream = new DataOutputViewStream(outputView);
        context.setOutput(new Output(outputViewStream));
        context.setPreviousOut(outputView);
      }

      output = context.output;
      context.kryo.writeClassAndObject(output, record);

      output.flush();
    } catch (KryoException e) {
      if (output != null) {
        output.clear();
      }
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T deserialize(DataInputView inputView) throws IOException {
    KryoThreadLocalContext context = kryoThreadLocalContext.get();
    Input input = null;

    try {
      if (inputView != context.previousInput) {
        DataInputViewStream inputViewStream = new DataInputViewStream(inputView);
        context.setInput(new NoFetchingInput(inputViewStream));
        context.setPreviousInput(inputView);
      }

      input = context.input;
      return (T) context.kryo.readClassAndObject(input);
    } catch (KryoException e) {
      throw e;
    }
  }

  @Override
  public TypeSerializer<T> duplicate() {
    return null;
  }

  private Kryo createKryoInstance() {
    Kryo kryo = new Kryo();

    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    kryo.setReferences(true);
    kryo.setInstantiatorStrategy(
        new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

    Serializer<?> serializer;
    if (kryoRegistrationList != null) {
      for (KryoRegistration registration : kryoRegistrationList) {
        serializer = registration.getSerializer(kryo);
        if (serializer == null) {
          kryo.register(registration.getRegisteredClass(), kryo.getNextRegistrationId());
        } else {
          kryo.register(
              registration.getRegisteredClass(), serializer, kryo.getNextRegistrationId());
        }
      }
    }

    return kryo;
  }

  @Override
  public T createInstance() {
    if (Modifier.isInterface(type.getModifiers()) || Modifier.isAbstract(type.getModifiers())) {
      return null;
    } else {
      return kryoThreadLocalContext.get().kryo.newInstance(type);
    }
  }

  public void cleanContext() {
    kryoThreadLocalContext.remove();
  }
}
