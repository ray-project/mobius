#pragma once

#include "common/status.h"
#include "config/streaming_config.h"
#include "queue/queue_handler.h"
#include "ring_buffer/ring_buffer.h"
#include "util/config.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

using namespace ray::core;

enum class TransferCreationStatus : uint32_t {
  FreshStarted = 0,
  PullOk = 1,
  Timeout = 2,
  DataLost = 3,
  Invalid = 999,
};

struct StreamingQueueInfo {
  // First message id in channel.
  uint64_t first_seq_id = 0;
  // Last message id in channel.
  uint64_t last_message_id = 0;
  // Target message id for flow control with consuemd window size.
  uint64_t target_message_id = 0;
  // Message id in last upstream notification.
  uint64_t consumed_message_id = 0;
  // Last bundle id consumed by downstream.
  uint64_t consumed_bundle_id = std::numeric_limits<uint64_t>::max();
};

struct ChannelCreationParameter {
  ActorID actor_id;
  std::shared_ptr<RayFunction> async_function;
  std::shared_ptr<RayFunction> sync_function;
};

/// PrducerChannelinfo and ConsumerChannelInfo contains channel information and
/// its metrics that help us to debug or show important messages in logging.
struct ProducerChannelInfo {
  ObjectID channel_id;
  StreamingRingBufferPtr writer_ring_buffer;
  uint64_t current_message_id;
  // Lastest bundle id of last bundle.
  uint64_t current_bundle_id;
  uint64_t message_last_commit_id;
  StreamingQueueInfo queue_info;
  uint32_t queue_size;
  int64_t message_pass_by_ts;
  ChannelCreationParameter parameter;

  /// The following parameters are used for event driven to record different
  /// input events.
  uint64_t sent_empty_cnt = 0;
  uint64_t flow_control_cnt = 0;
  uint64_t user_event_cnt = 0;
  uint64_t rb_full_cnt = 0;
  uint64_t queue_full_cnt = 0;
  uint64_t in_event_queue_cnt = 0;
  bool in_event_queue = false;
  bool flow_control = false;
};

struct ConsumerChannelInfo {
  ObjectID channel_id;
  uint64_t current_message_id;
  uint64_t barrier_id;
  uint64_t partial_barrier_id;

  StreamingQueueInfo queue_info;

  uint64_t last_queue_item_delay = 0;
  uint64_t last_queue_item_latency = 0;
  uint64_t last_queue_target_diff = 0;
  uint64_t get_queue_item_times = 0;
  ChannelCreationParameter parameter;
  // Total count of notify request.
  uint64_t notify_cnt = 0;
  uint64_t resend_notify_timer;
};

/// Two types of channel are presented:
///   * ProducerChannel is supporting all writing operations for upperlevel.
///   * ConsumerChannel is for all reader operations.
///  They share similar interfaces:
///    * ClearTransferCheckpoint(it's empty and unsupported now, we will add
///      implementation in next PR)
///    * NotifychannelConsumed (notify owner of channel which range data should
//       be release to avoid out of memory)
///  but some differences in read/write function.(named ProduceItemTochannel and
///  ConsumeItemFrom channel)
class ProducerChannel {
 public:
  explicit ProducerChannel(std::shared_ptr<Config> &transfer_config,
                           ProducerChannelInfo &p_channel_info);
  virtual ~ProducerChannel() = default;
  virtual StreamingStatus CreateTransferChannel() = 0;
  virtual StreamingStatus DestroyTransferChannel() = 0;
  virtual StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                                  uint64_t checkpoint_offset) = 0;
  virtual StreamingStatus RefreshChannelInfo() = 0;
  virtual StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size) = 0;
  virtual StreamingStatus NotifyChannelConsumed(uint64_t channel_offset) = 0;
  virtual uint64_t GetLastBundleId() const = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
  ProducerChannelInfo &channel_info_;
};

class ConsumerChannel {
 public:
  explicit ConsumerChannel(std::shared_ptr<Config> &transfer_config,
                           ConsumerChannelInfo &c_channel_info);
  virtual ~ConsumerChannel() = default;
  virtual TransferCreationStatus CreateTransferChannel() = 0;
  virtual StreamingStatus DestroyTransferChannel() = 0;
  virtual StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                                  uint64_t checkpoint_offset) = 0;
  virtual StreamingStatus RefreshChannelInfo() = 0;
  virtual StreamingStatus ConsumeItemFromChannel(std::shared_ptr<DataBundle> &message,
                                                 uint32_t timeout) = 0;
  virtual StreamingStatus NotifyChannelConsumed(uint64_t offset_id) = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
  ConsumerChannelInfo &channel_info_;
};

class StreamingQueueProducer : public ProducerChannel {
 public:
  explicit StreamingQueueProducer(std::shared_ptr<Config> &transfer_config,
                                  ProducerChannelInfo &p_channel_info);
  ~StreamingQueueProducer() override;
  StreamingStatus CreateTransferChannel() override;
  StreamingStatus DestroyTransferChannel() override;
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override;
  StreamingStatus RefreshChannelInfo() override;
  StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;
  uint64_t GetLastBundleId() const override;

 private:
  StreamingStatus CreateQueue();
  Status PushQueueItem(uint8_t *data, uint32_t data_size, uint64_t timestamp,
                       uint64_t msg_id_start, uint64_t msg_id_end);

 private:
  std::shared_ptr<WriterQueue> queue_;
};

class StreamingQueueConsumer : public ConsumerChannel {
 public:
  explicit StreamingQueueConsumer(std::shared_ptr<Config> &transfer_config,
                                  ConsumerChannelInfo &c_channel_info);
  ~StreamingQueueConsumer() override;
  TransferCreationStatus CreateTransferChannel() override;
  StreamingStatus DestroyTransferChannel() override;
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override;
  StreamingStatus RefreshChannelInfo() override;
  StreamingStatus ConsumeItemFromChannel(std::shared_ptr<DataBundle> &message,
                                         uint32_t timeout) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;

 private:
  StreamingQueueStatus GetQueue(const ObjectID &queue_id, uint64_t start_msg_id,
                                const ChannelCreationParameter &init_param);

 private:
  std::shared_ptr<ReaderQueue> queue_;
};

/// MockProducer and Mockconsumer are independent implementation of channels that
/// conduct a very simple memory channel for unit tests or intergation test.
class MockProducer : public ProducerChannel {
 public:
  explicit MockProducer(std::shared_ptr<Config> &transfer_config,
                        ProducerChannelInfo &channel_info)
      : ProducerChannel(transfer_config, channel_info), current_bundle_id_(0){};
  StreamingStatus CreateTransferChannel() override;

  StreamingStatus DestroyTransferChannel() override;

  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }

  StreamingStatus RefreshChannelInfo() override;

  StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size) override;

  StreamingStatus NotifyChannelConsumed(uint64_t channel_offset) override {
    return StreamingStatus::OK;
  }
  uint64_t GetLastBundleId() const override { return current_bundle_id_; }

 private:
  uint64_t current_bundle_id_;
};

class MockConsumer : public ConsumerChannel {
 public:
  explicit MockConsumer(std::shared_ptr<Config> &transfer_config,
                        ConsumerChannelInfo &c_channel_info)
      : ConsumerChannel(transfer_config, c_channel_info){};
  TransferCreationStatus CreateTransferChannel() override {
    return TransferCreationStatus::PullOk;
  }
  StreamingStatus DestroyTransferChannel() override { return StreamingStatus::OK; }
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }
  StreamingStatus RefreshChannelInfo() override;
  StreamingStatus ConsumeItemFromChannel(std::shared_ptr<DataBundle> &message,
                                         uint32_t timeout) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;
};

}  // namespace streaming
}  // namespace ray
