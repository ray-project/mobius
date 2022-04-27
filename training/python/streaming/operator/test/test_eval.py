import os
import logging
from time import sleep
from ray.streaming import StreamingContext
from ray.streaming.tests import test_utils
from ray.streaming.function import SourceFunction
from ray.test_utils import wait_for_condition
from streaming.operator.impl.eval_function import EvalSinkFunction


sink_file = "/tmp/test_eval.txt"

logger = logging.getLogger(__name__)


@test_utils.skip_if_no_streaming_jar()
def test_eval_word_count():
    class MockSourceFunction(SourceFunction):

        def init(self, parallel, index):
            self.tot = 10

        def fetch(self, ctx, checkpoint_id):
            if self.tot > 5:
                self.tot = self.tot - 1
                ctx.collect(bytes("a", encoding="utf8"))
            elif self.tot > 0:
                self.tot = self.tot - 1
                ctx.collect(bytes("b", encoding="utf8"))

    class MockEvalSinkFunction(EvalSinkFunction):

        def _evaluate(self):
            sleep(5)
            data_list = self._reader.fetch_data(10)
            with open(sink_file, "a") as f:
                for data in data_list:
                    print(data)
                    f.write(data)

    test_utils.start_ray()
    if os.path.exists(sink_file):
        os.remove(sink_file)

    ctx = StreamingContext.Builder() \
        .build()

    a = MockSourceFunction()
    b = MockEvalSinkFunction()
    ctx.source(a) \
       .disable_chain() \
       .sink(b) \
       .with_config(conf={"reader_max_slot_size": "10"})

    ctx.submit("test_eval_word_count")

    def check_succeed():
        if os.path.exists(sink_file):
            print("job finish.")
            with open(sink_file) as f:
                result = f.read()
                print(f"result {result}")
                return result.count("a") == 5 and result.count("b") == 5
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
