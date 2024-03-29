#include "queue/transport.h"

#include "queue/utils.h"
#include "ray/common/common_protocol.h"
#include "ray/internal/internal.h"

namespace ray {
namespace streaming {

static constexpr int TASK_OPTION_RETURN_NUM_0 = 0;
static constexpr int TASK_OPTION_RETURN_NUM_1 = 1;

void Transport::Send(std::shared_ptr<LocalMemoryBuffer> buffer) {
  STREAMING_LOG(DEBUG) << "Transport::Send buffer size: " << buffer->Size();
  RAY_UNUSED(ray::internal::SendInternal(peer_actor_id_, std::move(buffer), async_func_,
                                         TASK_OPTION_RETURN_NUM_0));
}

std::shared_ptr<LocalMemoryBuffer> Transport::SendForResult(
    std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms) {
  auto return_refs = ray::internal::SendInternal(peer_actor_id_, buffer, sync_func_,
                                                 TASK_OPTION_RETURN_NUM_1);
  auto return_ids = ObjectRefsToIds(return_refs);

  std::vector<std::shared_ptr<RayObject>> results;
  Status get_st =
      CoreWorkerProcess::GetCoreWorker().Get(return_ids, timeout_ms, &results);
  if (!get_st.ok()) {
    STREAMING_LOG(ERROR) << "Get fail.";
    return nullptr;
  }
  STREAMING_CHECK(results.size() >= 1);
  if (results[0]->IsException()) {
    STREAMING_LOG(ERROR) << "peer actor may has exceptions, should retry.";
    return nullptr;
  }
  STREAMING_CHECK(results[0]->HasData());
  if (results[0]->GetData()->Size() == 4) {
    STREAMING_LOG(WARNING) << "peer actor may not ready yet, should retry.";
    return nullptr;
  }

  std::shared_ptr<Buffer> result_buffer = results[0]->GetData();
  std::shared_ptr<LocalMemoryBuffer> return_buffer = std::make_shared<LocalMemoryBuffer>(
      result_buffer->Data(), result_buffer->Size(), true);
  return return_buffer;
}

std::shared_ptr<LocalMemoryBuffer> Transport::SendForResultWithRetry(
    std::shared_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms) {
  STREAMING_LOG(INFO) << "SendForResultWithRetry retry_cnt: " << retry_cnt
                      << " timeout_ms: " << timeout_ms;
  std::shared_ptr<LocalMemoryBuffer> buffer_shared = std::move(buffer);
  for (int cnt = 0; cnt < retry_cnt; cnt++) {
    auto result = SendForResult(buffer_shared, timeout_ms);
    if (result != nullptr) {
      return result;
    }
  }

  STREAMING_LOG(WARNING) << "SendForResultWithRetry fail after retry.";
  return nullptr;
}

}  // namespace streaming
}  // namespace ray
