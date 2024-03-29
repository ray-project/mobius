#pragma once

#include "protobuf/streaming_queue.pb.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

/// Base class of all message classes.
/// All payloads transferred through direct actor call are packed into a unified package,
/// consisting of protobuf-formatted metadata and data, including data and control
/// messages. These message classes wrap the package defined in
/// protobuf/streaming_queue.proto respectively.
class Message {
 public:
  /// Construct a Message instance.
  /// \param[in] actor_id ActorID of message sender.
  /// \param[in] peer_actor_id ActorID of message receiver.
  /// \param[in] queue_id queue id to identify which queue the message is sent to.
  /// \param[in] buffer an optional param, a chunk of data to send.
  Message(const ActorID &actor_id, const ActorID &peer_actor_id, const ObjectID &queue_id,
          std::shared_ptr<LocalMemoryBuffer> buffer = nullptr)
      : actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        buffer_(buffer) {}
  Message() {}
  virtual ~Message() {}
  inline ActorID ActorId() { return actor_id_; }
  inline ActorID PeerActorId() { return peer_actor_id_; }
  inline ObjectID QueueId() { return queue_id_; }
  inline std::shared_ptr<LocalMemoryBuffer> Buffer() { return buffer_; }

  /// Serialize all meta data and data to a LocalMemoryBuffer, which can be sent through
  /// direct actor call. \return serialized buffer .
  std::unique_ptr<LocalMemoryBuffer> ToBytes();

  /// Get message type.
  /// \return message type.
  virtual queue::protobuf::StreamingQueueMessageType Type() = 0;

  /// All subclasses should implement `ToProtobuf` to serialize its own protobuf data.
  virtual void ToProtobuf(std::string *output) = 0;

  void FillMessageCommon(queue::protobuf::MessageCommon *common);

 protected:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  std::shared_ptr<LocalMemoryBuffer> buffer_;

 public:
  /// A magic number to identify a valid message.
  static const uint32_t MagicNum;
};

/// MagicNum + MessageType
constexpr uint32_t kItemMetaHeaderSize =
    sizeof(Message::MagicNum) + sizeof(queue::protobuf::StreamingQueueMessageType);
/// kItemMetaHeaderSize + fbs length
constexpr uint32_t kItemHeaderSize = kItemMetaHeaderSize + sizeof(uint64_t);

/// Wrap StreamingQueueDataMsg in streaming_queue.proto.
/// DataMessage encapsulates the memory buffer of QueueItem, a one-to-one relationship
/// exists between DataMessage and QueueItem.
class DataMessage : public Message {
 public:
  DataMessage(const ActorID &actor_id, const ActorID &peer_actor_id, ObjectID queue_id,
              uint64_t seq_id, uint64_t msg_id_start, uint64_t msg_id_end,
              std::shared_ptr<LocalMemoryBuffer> buffer, bool raw)
      : Message(actor_id, peer_actor_id, queue_id, buffer),
        seq_id_(seq_id),
        msg_id_start_(msg_id_start),
        msg_id_end_(msg_id_end),
        raw_(raw) {}
  virtual ~DataMessage() {}

  static std::shared_ptr<DataMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t MsgIdStart() { return msg_id_start_; }
  inline uint64_t MsgIdEnd() { return msg_id_end_; }
  inline bool IsRaw() { return raw_; }
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t seq_id_;
  uint64_t msg_id_start_;
  uint64_t msg_id_end_;
  bool raw_;

  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueueDataMsgType;
};

/// Wrap StreamingQueueNotificationMsg in streaming_queue.proto.
/// NotificationMessage, downstream queues sends to upstream queues, for the data reader
/// to inform the data writer of the consumed offset.
class NotificationMessage : public Message {
 public:
  NotificationMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t msg_id, uint64_t bundle_id)
      : Message(actor_id, peer_actor_id, queue_id),
        msg_id_(msg_id),
        bundle_id_(bundle_id) {}

  virtual ~NotificationMessage() {}

  static std::shared_ptr<NotificationMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);

  inline uint64_t MsgId() { return msg_id_; }
  inline uint64_t BundleId() { return bundle_id_; }
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t msg_id_;
  uint64_t bundle_id_;
  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueueNotificationMsgType;
};

/// Wrap StreamingQueueCheckMsg in streaming_queue.proto.
/// CheckMessage, upstream queues sends to downstream queues, fot the data writer to check
/// whether the corresponded downstream queue is read or not.
class CheckMessage : public Message {
 public:
  CheckMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
               const ObjectID &queue_id)
      : Message(actor_id, peer_actor_id, queue_id) {}
  virtual ~CheckMessage() {}

  static std::shared_ptr<CheckMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);

  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckMsgType;
};

/// Wrap StreamingQueueCheckRspMsg in streaming_queue.proto.
/// CheckRspMessage, downstream queues sends to upstream queues, the response message to
/// CheckMessage to indicate whether downstream queue is ready or not.
class CheckRspMessage : public Message {
 public:
  CheckRspMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                  const ObjectID &queue_id, queue::protobuf::StreamingQueueError err_code)
      : Message(actor_id, peer_actor_id, queue_id), err_code_(err_code) {}
  virtual ~CheckRspMessage() {}

  static std::shared_ptr<CheckRspMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  inline queue::protobuf::StreamingQueueError Error() { return err_code_; }

 private:
  queue::protobuf::StreamingQueueError err_code_;
  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueueCheckRspMsgType;
};

class PullRequestMessage : public Message {
 public:
  PullRequestMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                     const ObjectID &queue_id, uint64_t msg_id)
      : Message(actor_id, peer_actor_id, queue_id), msg_id_(msg_id) {}
  virtual ~PullRequestMessage() {}

  static std::shared_ptr<PullRequestMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  inline uint64_t MsgId() { return msg_id_; }
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t msg_id_;
  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueuePullRequestMsgType;
};

class PullResponseMessage : public Message {
 public:
  PullResponseMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                      const ObjectID &queue_id, uint64_t seq_id, uint64_t msg_id,
                      queue::protobuf::StreamingQueueError err_code,
                      bool is_upstream_first_pull)
      : Message(actor_id, peer_actor_id, queue_id),
        seq_id_(seq_id),
        msg_id_(msg_id),
        is_upstream_first_pull_(is_upstream_first_pull),
        err_code_(err_code) {}
  virtual ~PullResponseMessage() = default;

  static std::shared_ptr<PullResponseMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t MsgId() { return msg_id_; }
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  inline queue::protobuf::StreamingQueueError Error() { return err_code_; }
  inline bool IsUpstreamFirstPull() { return is_upstream_first_pull_; }

 private:
  uint64_t seq_id_;
  uint64_t msg_id_;
  bool is_upstream_first_pull_;
  queue::protobuf::StreamingQueueError err_code_;
  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueuePullResponseMsgType;
};

class ResendDataMessage : public Message {
 public:
  ResendDataMessage(const ActorID &actor_id, const ActorID &peer_actor_id,
                    ObjectID queue_id, uint64_t first_seq_id, uint64_t seq_id,
                    uint64_t msg_id_start, uint64_t msg_id_end, uint64_t last_seq_id,
                    std::shared_ptr<LocalMemoryBuffer> buffer, bool raw)
      : Message(actor_id, peer_actor_id, queue_id, buffer),
        first_seq_id_(first_seq_id),
        last_seq_id_(last_seq_id),
        seq_id_(seq_id),
        msg_id_start_(msg_id_start),
        msg_id_end_(msg_id_end),
        raw_(raw) {}
  virtual ~ResendDataMessage() {}

  static std::shared_ptr<ResendDataMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  inline uint64_t MsgIdStart() { return msg_id_start_; }
  inline uint64_t MsgIdEnd() { return msg_id_end_; }
  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t FirstSeqId() { return first_seq_id_; }
  inline uint64_t LastSeqId() { return last_seq_id_; }
  inline bool IsRaw() { return raw_; }
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }

 private:
  uint64_t first_seq_id_;
  uint64_t last_seq_id_;
  uint64_t seq_id_;
  uint64_t msg_id_start_;
  uint64_t msg_id_end_;
  bool raw_;

  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueueResendDataMsgType;
};

/// Wrap StreamingQueueTestInitMsg in streaming_queue.proto.
/// TestInitMessage, used for test, driver sends to test workers to init test suite.
class TestInitMessage : public Message {
 public:
  TestInitMessage(const queue::protobuf::StreamingQueueTestRole role,
                  const ActorID &actor_id, const ActorID &peer_actor_id,
                  const std::string actor_handle_serialized,
                  const std::vector<ObjectID> &queue_ids,
                  const std::vector<ObjectID> &rescale_queue_ids,
                  std::string test_suite_name, std::string test_name, uint64_t param)
      : Message(actor_id, peer_actor_id, queue_ids[0]),
        actor_handle_serialized_(actor_handle_serialized),
        queue_ids_(queue_ids),
        rescale_queue_ids_(rescale_queue_ids),
        role_(role),
        test_suite_name_(test_suite_name),
        test_name_(test_name),
        param_(param) {}
  virtual ~TestInitMessage() {}

  static std::shared_ptr<TestInitMessage> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  inline std::string ActorHandleSerialized() { return actor_handle_serialized_; }
  inline queue::protobuf::StreamingQueueTestRole Role() { return role_; }
  inline std::vector<ObjectID> QueueIds() { return queue_ids_; }
  inline std::vector<ObjectID> RescaleQueueIds() { return rescale_queue_ids_; }
  inline std::string TestSuiteName() { return test_suite_name_; }
  inline std::string TestName() { return test_name_; }
  inline uint64_t Param() { return param_; }

  std::string ToString() {
    std::ostringstream os;
    os << "actor_handle_serialized: " << actor_handle_serialized_;
    os << " actor_id: " << ActorId();
    os << " peer_actor_id: " << PeerActorId();
    os << " queue_ids:[";
    for (auto &qid : queue_ids_) {
      os << qid << ",";
    }
    os << "], rescale_queue_ids:[";
    for (auto &qid : rescale_queue_ids_) {
      os << qid << ",";
    }
    os << "],";
    os << " role:" << queue::protobuf::StreamingQueueTestRole_Name(role_);
    os << " suite_name: " << test_suite_name_;
    os << " test_name: " << test_name_;
    os << " param: " << param_;
    return os.str();
  }

 private:
  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueueTestInitMsgType;
  std::string actor_handle_serialized_;
  std::vector<ObjectID> queue_ids_;
  std::vector<ObjectID> rescale_queue_ids_;
  queue::protobuf::StreamingQueueTestRole role_;
  std::string test_suite_name_;
  std::string test_name_;
  uint64_t param_;
};

/// Wrap StreamingQueueTestCheckStatusRspMsg in streaming_queue.proto.
/// TestCheckStatusRspMsg, used for test, driver sends to test workers to check
/// whether test has completed or failed.
class TestCheckStatusRspMsg : public Message {
 public:
  TestCheckStatusRspMsg(const std::string test_name, bool status)
      : test_name_(test_name), status_(status) {}
  virtual ~TestCheckStatusRspMsg() {}

  static std::shared_ptr<TestCheckStatusRspMsg> FromBytes(uint8_t *bytes);
  virtual void ToProtobuf(std::string *output);
  inline queue::protobuf::StreamingQueueMessageType Type() { return type_; }
  inline std::string TestName() { return test_name_; }
  inline bool Status() { return status_; }

 private:
  const queue::protobuf::StreamingQueueMessageType type_ =
      queue::protobuf::StreamingQueueMessageType::StreamingQueueTestCheckStatusRspMsgType;
  std::string test_name_;
  bool status_;
};

}  // namespace streaming
}  // namespace ray
