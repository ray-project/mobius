#pragma once

#include "ray/util/logging.h"

#define STREAMING_LOG RAY_LOG
#define STREAMING_CHECK RAY_CHECK
namespace ray {
namespace streaming {}  // namespace streaming
// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define STREAMING_CHECK_OK_PREPEND(to_call, msg)                                         \
  do {                                                                                   \
    ::ray::streaming::StreamingStatus _s = (to_call);                                    \
    STREAMING_CHECK(_s == ::ray::streaming::StreamingStatus::OK) << (msg) << ": " << _s; \
  } while (0)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define STREAMING_CHECK_OK(s) STREAMING_CHECK_OK_PREPEND(s, "Bad status")
}  // namespace ray
