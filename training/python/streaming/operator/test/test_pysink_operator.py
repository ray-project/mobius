import ray
from ray.streaming import StreamingContext
from ray.streaming.function import SourceFunction
from ray.streaming.tests import test_utils
from streaming.operator.impl.tf_function import TFSinkFunction
import random


@test_utils.skip_if_no_streaming_jar()
def test_pysink_word_count():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()
    tf_sink_function = TFSinkFunction()
    ctx.read_text_file(__file__) \
        .set_parallelism(1) \
        .flat_map(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .filter(lambda x: "ray" not in x) \
        .map(lambda x : bytes(x[0], encoding='utf-8')) \
        .disable_chain() \
        .sink(tf_sink_function) \
        .with_config(conf={"operator_module_path" : ".", "operator_module_name" : "streaming.operator.impl.tf_operator", "operator_class_name" : "Mock4RescaleOperator"})
    ctx.submit("tf_function")
    import time
    time.sleep(10)
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_is_ready_rescaling():
    test_utils.start_ray()
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()
    tf_sink_function = TFSinkFunction()
    ctx.read_text_file(__file__) \
        .disable_chain() \
        .sink(tf_sink_function) \
        .with_config(conf={"operator_module_path" : ".", "operator_module_name" : "streaming.operator.impl.tf_operator", "operator_class_name" : "Mock4RescaleOperator"})
    ctx.submit("test_is_ready_rescaling")
    import time
    time.sleep(10)
    actor_name_1 = "1-PythonOperator-0|0"
    actor_name_2 = "2-PythonOperator-0|1"
    actor_1 = ray.get_actor(actor_name_1)
    actor_2 = ray.get_actor(actor_name_2)
    object_ref_1 = actor_1.is_ready_rescaling.remote()
    object_ref_2 = actor_2.is_ready_rescaling.remote()
    assert ray.get(object_ref_1)
    assert ray.get(object_ref_2)
    ray.shutdown()


@test_utils.skip_if_no_streaming_jar()
def test_tf_function_state():
    class TestSource(SourceFunction):
        def init(self, parallel, index):
            pass

        def fetch(self, ctx, checkpoint_id):
            import time
            time.sleep(0.1)
            ctx.collect("hello ray {}".format(random.randint(0, 100)))

    test_utils.start_ray()
    ctx = StreamingContext.Builder().build()
    tf_sink_function = TFSinkFunction()
    ctx.source(TestSource()) \
        .set_parallelism(1) \
        .flat_map(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .filter(lambda x: "ray" not in x) \
        .map(lambda x : bytes(x[0], encoding='utf-8')) \
        .disable_chain() \
        .sink(tf_sink_function) \
        .with_config(conf={"operator_module_path" : ".", "operator_module_name" : "streaming.operator.impl.tf_operator", "operator_class_name" : "Mock4RescaleOperator"})
    ctx.submit("tf_function_state_{}".format(random.random()))
    import time
    time.sleep(30)
    ray.shutdown()
