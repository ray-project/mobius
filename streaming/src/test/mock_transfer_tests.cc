#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"

using namespace ray;
using namespace ray::streaming;

class MockDataWriter : public DataWriter {
 public:
  MockDataWriter(std::shared_ptr<RuntimeContext> &runtime_context)
      : DataWriter(runtime_context) {}
  uint64_t GetSendEmptyCnt(const ObjectID &queue_id) {
    return channel_info_map_[queue_id].sent_empty_cnt;
  }
};

TEST(StreamingMockTransfer, mock_produce_consume) {
  std::shared_ptr<Config> transfer_config;
  ObjectID channel_id = ObjectID::FromRandom();
  ProducerChannelInfo producer_channel_info;
  producer_channel_info.channel_id = channel_id;
  producer_channel_info.current_message_id = 0;
  MockProducer producer(transfer_config, producer_channel_info);

  ConsumerChannelInfo consumer_channel_info;
  consumer_channel_info.channel_id = channel_id;
  MockConsumer consumer(transfer_config, consumer_channel_info);

  producer.CreateTransferChannel();
  uint8_t data[3] = {1, 2, 3};
  StreamingMessagePtr message =
      std::make_shared<StreamingMessage>(data, 3, 7, StreamingMessageType::Message);
  std::list<StreamingMessagePtr> message_list;
  message_list.push_back(message);
  StreamingMessageBundle bundle(message_list, 1, 1, StreamingMessageBundleType::Bundle);
  uint64_t bundle_size = bundle.ClassBytesSize();
  uint8_t *bundle_bytes = new uint8_t[bundle_size];
  bundle.ToBytes(bundle_bytes);

  producer.ProduceItemToChannel(bundle_bytes, bundle_size);

  std::shared_ptr<DataBundle> message_bundle(new DataBundle());
  consumer.ConsumeItemFromChannel(message_bundle, -1);
  EXPECT_EQ(message_bundle->data_size, bundle_size);
  EXPECT_EQ(std::memcmp(message_bundle->data, bundle_bytes, bundle_size), 0);
  consumer.NotifyChannelConsumed(1);

  delete[] bundle_bytes;
  auto status = consumer.ConsumeItemFromChannel(message_bundle, -1);
  EXPECT_EQ(status, StreamingStatus::NoSuchItem);
}

class StreamingTransferTest : public ::testing::Test {
 public:
  StreamingTransferTest() {
    writer_runtime_context = std::make_shared<RuntimeContext>();
    reader_runtime_context = std::make_shared<RuntimeContext>();
    writer_runtime_context->MarkMockTest();
    reader_runtime_context->MarkMockTest();
    writer = std::make_shared<MockDataWriter>(writer_runtime_context);
    reader = std::make_shared<DataReader>(reader_runtime_context);
  }
  virtual ~StreamingTransferTest() = default;
  void InitTransfer(int channel_num = 1) {
    for (int i = 0; i < channel_num; ++i) {
      queue_vec.push_back(ObjectID::FromRandom());
    }
    std::vector<uint64_t> channel_id_vec(queue_vec.size(), 0);
    std::vector<uint64_t> queue_size_vec(queue_vec.size(), 10000);
    std::vector<ChannelCreationParameter> params(queue_vec.size());
    std::vector<TransferCreationStatus> creation_status;
    writer->Init(queue_vec, params, channel_id_vec, queue_size_vec);
    reader->Init(queue_vec, params, channel_id_vec, creation_status, timer_interval);
  }
  void DestroyTransfer() {
    writer.reset();
    reader.reset();
  }

 protected:
  std::shared_ptr<MockDataWriter> writer;
  std::shared_ptr<DataReader> reader;
  std::vector<ObjectID> queue_vec;
  std::shared_ptr<RuntimeContext> writer_runtime_context;
  std::shared_ptr<RuntimeContext> reader_runtime_context;
  int timer_interval = -1;
};

TEST_F(StreamingTransferTest, exchange_single_channel_test) {
  InitTransfer();
  writer->Run();
  uint8_t data[4] = {1, 2, 3, 0xff};
  uint32_t data_size = 4;
  writer->WriteMessageToBufferRing(queue_vec[0], data, data_size);
  std::shared_ptr<DataBundle> msg;
  reader->GetBundle(5000, msg);
  STREAMING_LOG(INFO) << Util::Byte2hex(msg->data, msg->data_size);
  StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
  auto &message_list = bundle_ptr->GetMessageList();
  auto &message = message_list.front();
  EXPECT_EQ(std::memcmp(message->Payload(), data, data_size), 0);
}

TEST_F(StreamingTransferTest, exchange_multichannel_test) {
  int channel_num = 4;
  InitTransfer(4);
  writer->Run();
  for (int i = 0; i < channel_num; ++i) {
    uint8_t data[4] = {1, 2, 3, (uint8_t)i};
    uint32_t data_size = 4;
    writer->WriteMessageToBufferRing(queue_vec[i], data, data_size);
    std::shared_ptr<DataBundle> msg;
    reader->GetBundle(5000, msg);
    EXPECT_EQ(msg->from, queue_vec[i]);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
    auto &message_list = bundle_ptr->GetMessageList();
    auto &message = message_list.front();
    EXPECT_EQ(std::memcmp(message->Payload(), data, data_size), 0);
  }
}

TEST_F(StreamingTransferTest, exchange_consumed_test) {
  InitTransfer();
  writer->Run();
  uint32_t data_size = 8196;
  std::shared_ptr<uint8_t> data(new uint8_t[data_size]);
  auto func = [data, data_size](int index) { std::fill_n(data.get(), data_size, index); };

  size_t num = 40000;
  std::thread write_thread([this, data, data_size, &func, num]() {
    for (size_t i = 0; i < num; ++i) {
      func(i);
      writer->WriteMessageToBufferRing(queue_vec[0], data.get(), data_size);
    }
  });

  std::list<StreamingMessagePtr> read_message_list;
  while (read_message_list.size() < num) {
    std::shared_ptr<DataBundle> msg;
    reader->GetBundle(5000, msg);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
    auto &message_list = bundle_ptr->GetMessageList();
    std::copy(message_list.begin(), message_list.end(),
              std::back_inserter(read_message_list));
  }
  int index = 0;
  for (auto &message : read_message_list) {
    func(index++);
    EXPECT_EQ(std::memcmp(message->Payload(), data.get(), data_size), 0);
  }
  write_thread.join();
}

TEST_F(StreamingTransferTest, flow_control_test) {
  InitTransfer();
  writer->Run();
  uint32_t data_size = 8196;
  std::shared_ptr<uint8_t> data(new uint8_t[data_size]);
  auto func = [data, data_size](int index) { std::fill_n(data.get(), data_size, index); };

  size_t num = 10000;
  std::thread write_thread([this, data, data_size, &func, num]() {
    for (size_t i = 0; i < num; ++i) {
      func(i);
      writer->WriteMessageToBufferRing(queue_vec[0], data.get(), data_size);
    }
  });
  std::unordered_map<ObjectID, ProducerChannelInfo> *writer_offset_info = nullptr;
  std::unordered_map<ObjectID, ConsumerChannelInfo> *reader_offset_info = nullptr;
  writer->GetOffsetInfo(writer_offset_info);
  reader->GetOffsetInfo(reader_offset_info);
  uint32_t writer_step = writer_runtime_context->GetConfig().GetWriterConsumedStep();
  uint32_t reader_step = reader_runtime_context->GetConfig().GetReaderConsumedStep();
  uint64_t &writer_current_msg_id =
      (*writer_offset_info)[queue_vec[0]].current_message_id;
  uint64_t &writer_last_commit_id =
      (*writer_offset_info)[queue_vec[0]].message_last_commit_id;
  uint64_t &writer_target_msg_id =
      (*writer_offset_info)[queue_vec[0]].queue_info.target_message_id;
  uint64_t &reader_target_msg_id =
      (*reader_offset_info)[queue_vec[0]].queue_info.target_message_id;
  do {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(StreamingConfig::TIME_WAIT_UINT));
    STREAMING_LOG(INFO) << "Writer currrent msg id " << writer_current_msg_id
                        << ", writer target_msg_id=" << writer_target_msg_id
                        << ", consumer step " << writer_step;
  } while (writer_current_msg_id < writer_step);

  std::list<StreamingMessagePtr> read_message_list;
  while (read_message_list.size() < num) {
    std::shared_ptr<DataBundle> msg;
    reader->GetBundle(1000, msg);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
    auto &message_list = bundle_ptr->GetMessageList();
    std::copy(message_list.begin(), message_list.end(),
              std::back_inserter(read_message_list));
    ASSERT_GE(writer_step, writer_last_commit_id - msg->meta->GetLastMessageId());
    ASSERT_GE(msg->meta->GetLastMessageId() + reader_step, reader_target_msg_id);
  }
  int index = 0;
  for (auto &message : read_message_list) {
    func(index++);
    EXPECT_EQ(std::memcmp(message->Payload(), data.get(), data_size), 0);
  }
  write_thread.join();
}

TEST_F(StreamingTransferTest, empty_message_flow_control) {
  // NOTE(lingxuan.zlx): No empty message will be sent after version 2.1,
  // so we choose exectly once strategy to fit unit test.
  StreamingConfig config;
  config.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  config.SetEmptyMessageTimeInterval(5);
  config.SetWriterConsumedStep(5);
  config.SetBundleConsumedStep(5);
  config.SetReaderConsumedStep(2);
  timer_interval = 10;
  writer_runtime_context->SetConfig(config);
  InitTransfer();
  writer->Run();
  size_t reader_target_empty_cnt = 10;
  for (size_t i = 0; i < reader_target_empty_cnt; ++i) {
    std::shared_ptr<DataBundle> msg;
    while (StreamingStatus::OK != reader->GetBundle(12, msg)) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(msg && msg->meta && msg->meta->IsEmptyMsg());
  }
  STREAMING_LOG(INFO) << writer->GetSendEmptyCnt(queue_vec[0]) << " "
                      << reader_target_empty_cnt;
  EXPECT_TRUE(writer->GetSendEmptyCnt(queue_vec[0]) < 10 * reader_target_empty_cnt);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
