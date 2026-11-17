#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "source/extensions/clusters/redis/redis_cluster_lb.h"
#include "source/extensions/filters/network/common/redis/client.h"

#include "test/common/upstream/utility.h"
#include "test/mocks/common.h"
#include "test/mocks/upstream/cluster_info.h"
#include "test/mocks/upstream/priority_set.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/status_utility.h"

using testing::Return;

namespace Envoy {
namespace Extensions {
namespace Clusters {
namespace Redis {

class TestLoadBalancerContext : public RedisLoadBalancerContext,
                                public Upstream::LoadBalancerContextBase {
public:
  TestLoadBalancerContext(uint64_t hash_key, bool is_read,
                          NetworkFilters::Common::Redis::Client::ReadPolicy read_policy,
                          const std::string& client_zone = "")
      : hash_key_(hash_key), is_read_(is_read), read_policy_(read_policy),
        client_zone_(client_zone) {}

  TestLoadBalancerContext(std::optional<uint64_t> hash) : hash_key_(hash) {}

  // Upstream::LoadBalancerContext
  std::optional<uint64_t> computeHashKey() override { return hash_key_; }

  bool isReadCommand() const override { return is_read_; };
  NetworkFilters::Common::Redis::Client::ReadPolicy readPolicy() const override {
    return read_policy_;
  };
  const std::string& clientZone() const override { return client_zone_; }

  std::optional<uint64_t> hash_key_;
  bool is_read_{};
  NetworkFilters::Common::Redis::Client::ReadPolicy read_policy_{};
  std::string client_zone_;
};

class RedisClusterLoadBalancerTest : public Event::TestUsingSimulatedTime, public testing::Test {
public:
  RedisClusterLoadBalancerTest() = default;

  // Helper to create a host with locality zone set
  Upstream::HostSharedPtr makeHostWithZone(const std::string& url, const std::string& zone) {
    envoy::config::core::v3::Locality locality;
    locality.set_zone(zone);
    return Upstream::makeTestHost(info_, url, locality);
  }

  void init() {
    factory_ = std::make_shared<RedisClusterLoadBalancerFactory>(random_);
    lb_ = std::make_unique<RedisClusterThreadAwareLoadBalancer>(factory_);
    EXPECT_OK(lb_->initialize());
    factory_->onHostHealthUpdate();
  }

  void validateAssignment(Upstream::HostVector& hosts,
                          const std::vector<std::pair<uint32_t, uint32_t>>& expected_assignments,
                          bool read_command = false,
                          NetworkFilters::Common::Redis::Client::ReadPolicy read_policy =
                              NetworkFilters::Common::Redis::Client::ReadPolicy::Primary,
                          const std::string& client_zone = "") {

    Upstream::LoadBalancerPtr lb = lb_->factory()->create(lb_params_);
    for (auto& assignment : expected_assignments) {
      TestLoadBalancerContext context(assignment.first, read_command, read_policy, client_zone);
      auto host = lb->chooseHost(&context).host;
      EXPECT_FALSE(host == nullptr);
      EXPECT_EQ(hosts[assignment.second]->address()->asString(), host->address()->asString());
    }
  }

  static std::pair<std::string, Upstream::HostSharedPtr>
  makePair(const Upstream::HostSharedPtr& host) {
    return {host->address()->asString(), host};
  }

  Upstream::HostMap generateHostMap(Upstream::HostVector& hosts) {
    Upstream::HostMap map;
    std::transform(hosts.begin(), hosts.end(), std::inserter(map, map.end()), makePair);
    return map;
  }

  std::shared_ptr<RedisClusterLoadBalancerFactory> factory_;
  std::unique_ptr<RedisClusterThreadAwareLoadBalancer> lb_;
  std::shared_ptr<Upstream::MockClusterInfo> info_{new NiceMock<Upstream::MockClusterInfo>()};
  NiceMock<Random::MockRandomGenerator> random_;

  // Just use this as parameters of create() method but thread aware load balancer will not use it.
  NiceMock<Upstream::MockPrioritySet> worker_priority_set_;
  Upstream::LoadBalancerParams lb_params_{worker_priority_set_, {}};
};

class RedisLoadBalancerContextImplTest : public testing::Test {
public:
  void makeBulkStringArray(NetworkFilters::Common::Redis::RespValue& value,
                           const std::vector<std::string>& strings) {
    std::vector<NetworkFilters::Common::Redis::RespValue> values(strings.size());
    for (uint64_t i = 0; i < strings.size(); i++) {
      values[i].type(NetworkFilters::Common::Redis::RespType::BulkString);
      values[i].asString() = strings[i];
    }

    value.type(NetworkFilters::Common::Redis::RespType::Array);
    value.asArray().swap(values);
  }
};

// Works correctly without any hosts.
TEST_F(RedisClusterLoadBalancerTest, NoHost) {
  init();
  EXPECT_EQ(nullptr, lb_->factory()->create(lb_params_)->chooseHost(nullptr).host);
};

// Verify stub LB interface methods return expected defaults.
TEST_F(RedisClusterLoadBalancerTest, LoadBalancerStubMethods) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90")};
  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(
      std::vector<ClusterSlot>{ClusterSlot(0, 16383, hosts[0]->address())});
  Upstream::HostMap all_hosts = generateHostMap(hosts);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  Upstream::LoadBalancerPtr lb = lb_->factory()->create(lb_params_);

  // peekAnotherHost is not implemented and always returns nullptr.
  EXPECT_EQ(nullptr, lb->peekAnotherHost(nullptr));

  // selectExistingConnection is not implemented and always returns nullopt.
  std::vector<uint8_t> hash_key;
  EXPECT_EQ(std::nullopt, lb->selectExistingConnection(nullptr, *hosts[0], hash_key));

  // lifetimeCallbacks is not implemented and returns empty OptRef.
  EXPECT_FALSE(lb->lifetimeCallbacks().has_value());
}

// Works correctly with empty context
TEST_F(RedisClusterLoadBalancerTest, NoHash) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:92")};

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 1000, hosts[0]->address()),
      ClusterSlot(1001, 2000, hosts[1]->address()),
      ClusterSlot(2001, 16383, hosts[2]->address()),
  });
  Upstream::HostMap all_hosts{
      {hosts[0]->address()->asString(), hosts[0]},
      {hosts[1]->address()->asString(), hosts[1]},
      {hosts[2]->address()->asString(), hosts[2]},
  };
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);
  TestLoadBalancerContext context(std::nullopt);
  EXPECT_EQ(nullptr, lb_->factory()->create(lb_params_)->chooseHost(&context).host);
};

TEST_F(RedisClusterLoadBalancerTest, Basic) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:92")};

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 1000, hosts[0]->address()),
      ClusterSlot(1001, 2000, hosts[1]->address()),
      ClusterSlot(2001, 16383, hosts[2]->address()),
  });
  Upstream::HostMap all_hosts{
      {hosts[0]->address()->asString(), hosts[0]},
      {hosts[1]->address()->asString(), hosts[1]},
      {hosts[2]->address()->asString(), hosts[2]},
  };
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // A list of (hash: host_index) pair
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 0},    {100, 0},   {1000, 0}, {17382, 0}, {1001, 1},  {1100, 1},
      {2000, 1}, {18382, 1}, {2001, 2}, {2100, 2},  {16383, 2}, {19382, 2}};
  validateAssignment(hosts, expected_assignments);
}

TEST_F(RedisClusterLoadBalancerTest, Shard) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:92")};

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 1000, hosts[0]->address()),
      ClusterSlot(1001, 2000, hosts[1]->address()),
      ClusterSlot(2001, 16383, hosts[2]->address()),
  });
  Upstream::HostMap all_hosts{
      {hosts[0]->address()->asString(), hosts[0]},
      {hosts[1]->address()->asString(), hosts[1]},
      {hosts[2]->address()->asString(), hosts[2]},
  };
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // A list of (hash: host_index) pair
  // Simple read command
  std::vector<NetworkFilters::Common::Redis::RespValue> get_foo(2);
  get_foo[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[0].asString() = "get";
  get_foo[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[1].asString() = "foo";

  NetworkFilters::Common::Redis::RespValue get_request;
  get_request.type(NetworkFilters::Common::Redis::RespType::Array);
  get_request.asArray().swap(get_foo);

  Upstream::LoadBalancerPtr lb = lb_->factory()->create(lb_params_);
  for (uint16_t i = 0; i < 5; i++) {
    RedisSpecifyShardContextImpl context(i, get_request);
    auto host = lb->chooseHost(&context).host;
    if (i < 3) {
      EXPECT_FALSE(host == nullptr);
      EXPECT_EQ(hosts[i]->address()->asString(), host->address()->asString());
    } else {
      EXPECT_TRUE(host == nullptr);
    }
  }
}

TEST_F(RedisClusterLoadBalancerTest, ReadStrategiesHealthy) {
  Upstream::HostVector hosts{
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:90"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:91"),
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 2000, hosts[0]->address()),
      ClusterSlot(2001, 16383, hosts[1]->address()),
  });
  slots->at(0).addReplica(hosts[2]->address());
  slots->at(1).addReplica(hosts[3]->address());
  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // A list of (hash: host_index) pair
  const std::vector<std::pair<uint32_t, uint32_t>> replica_assignments = {
      {0, 2}, {1100, 2}, {2000, 2}, {18382, 2}, {2001, 3}, {2100, 3}, {16383, 3}, {19382, 3}};
  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Replica);
  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferReplica);

  const std::vector<std::pair<uint32_t, uint32_t>> primary_assignments = {
      {0, 0}, {1100, 0}, {2000, 0}, {18382, 0}, {2001, 1}, {2100, 1}, {16383, 1}, {19382, 1}};
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferPrimary);

  ON_CALL(random_, random()).WillByDefault(Return(0));
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Any);
  ON_CALL(random_, random()).WillByDefault(Return(1));
  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Any);
}

TEST_F(RedisClusterLoadBalancerTest, ReadStrategiesUnhealthyPrimary) {
  Upstream::HostVector hosts{
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:90"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:91"),
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 2000, hosts[0]->address()),
      ClusterSlot(2001, 16383, hosts[1]->address()),
  });
  slots->at(0).addReplica(hosts[2]->address());
  slots->at(1).addReplica(hosts[3]->address());
  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  hosts[0]->healthFlagSet(Upstream::Host::HealthFlag::FAILED_ACTIVE_HC);
  hosts[1]->healthFlagSet(Upstream::Host::HealthFlag::FAILED_ACTIVE_HC);

  factory_->onHostHealthUpdate();

  // A list of (hash: host_index) pair
  const std::vector<std::pair<uint32_t, uint32_t>> replica_assignments = {
      {0, 2}, {1100, 2}, {2000, 2}, {18382, 2}, {2001, 3}, {2100, 3}, {16383, 3}, {19382, 3}};
  const std::vector<std::pair<uint32_t, uint32_t>> primary_assignments = {
      {0, 0}, {1100, 0}, {2000, 0}, {18382, 0}, {2001, 1}, {2100, 1}, {16383, 1}, {19382, 1}};

  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Replica);
  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferReplica);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);
  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferPrimary);

  ON_CALL(random_, random()).WillByDefault(Return(0));
  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Any);
  ON_CALL(random_, random()).WillByDefault(Return(1));
  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Any);
}

TEST_F(RedisClusterLoadBalancerTest, ReadStrategiesUnhealthyReplica) {
  Upstream::HostVector hosts{
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:90"),
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:91"),
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 2000, hosts[0]->address()),
      ClusterSlot(2001, 16383, hosts[1]->address()),
  });
  slots->at(0).addReplica(hosts[2]->address());
  slots->at(1).addReplica(hosts[3]->address());
  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  hosts[2]->healthFlagSet(Upstream::Host::HealthFlag::FAILED_ACTIVE_HC);
  hosts[3]->healthFlagSet(Upstream::Host::HealthFlag::FAILED_ACTIVE_HC);

  factory_->onHostHealthUpdate();

  // A list of (hash: host_index) pair
  const std::vector<std::pair<uint32_t, uint32_t>> replica_assignments = {
      {0, 2}, {1100, 2}, {2000, 2}, {18382, 2}, {2001, 3}, {2100, 3}, {16383, 3}, {19382, 3}};
  const std::vector<std::pair<uint32_t, uint32_t>> primary_assignments = {
      {0, 0}, {1100, 0}, {2000, 0}, {18382, 0}, {2001, 1}, {2100, 1}, {16383, 1}, {19382, 1}};

  validateAssignment(hosts, replica_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Replica);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferReplica);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferPrimary);

  ON_CALL(random_, random()).WillByDefault(Return(0));
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Any);
  ON_CALL(random_, random()).WillByDefault(Return(1));
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Any);
}

TEST_F(RedisClusterLoadBalancerTest, ReadStrategiesNoReplica) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91")};

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 2000, hosts[0]->address()),
      ClusterSlot(2001, 16383, hosts[1]->address()),
  });
  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // A list of (hash: host_index) pair
  const std::vector<std::pair<uint32_t, uint32_t>> primary_assignments = {
      {0, 0}, {1100, 0}, {2000, 0}, {18382, 0}, {2001, 1}, {2100, 1}, {16383, 1}, {19382, 1}};
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferPrimary);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::Any);
  validateAssignment(hosts, primary_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::PreferReplica);

  Upstream::LoadBalancerPtr lb = lb_->factory()->create(lb_params_);
  TestLoadBalancerContext context(1100, true,
                                  NetworkFilters::Common::Redis::Client::ReadPolicy::Replica);
  auto host = lb->chooseHost(&context).host;
  EXPECT_TRUE(host == nullptr);
}

TEST_F(RedisClusterLoadBalancerTest, ClusterSlotUpdate) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91")};
  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 1000, hosts[0]->address()), ClusterSlot(1001, 16383, hosts[1]->address())});
  Upstream::HostMap all_hosts{{hosts[0]->address()->asString(), hosts[0]},
                              {hosts[1]->address()->asString(), hosts[1]}};
  init();
  EXPECT_EQ(true, factory_->onClusterSlotUpdate(std::move(slots), all_hosts));

  // A list of initial (hash: host_index) pair
  const std::vector<std::pair<uint32_t, uint32_t>> original_assignments = {
      {100, 0}, {1100, 1}, {2100, 1}};

  validateAssignment(hosts, original_assignments);

  // Update the slot allocation should also change the assignment.
  std::vector<ClusterSlot> updated_slot{
      ClusterSlot(0, 1000, hosts[0]->address()),
      ClusterSlot(1001, 2000, hosts[1]->address()),
      ClusterSlot(2001, 16383, hosts[0]->address()),
  };
  EXPECT_EQ(true, factory_->onClusterSlotUpdate(
                      std::make_unique<std::vector<ClusterSlot>>(updated_slot), all_hosts));

  // A list of updated (hash: host_index) pair.
  const std::vector<std::pair<uint32_t, uint32_t>> updated_assignments = {
      {100, 0}, {1100, 1}, {2100, 0}};
  validateAssignment(hosts, updated_assignments);
}

// Verifies that a worker-local LB instance refreshes its slot and shard
// snapshot when the worker priority set fires its member update callback,
// rather than relying on the cluster manager to recreate the LB.
TEST_F(RedisClusterLoadBalancerTest, LoadBalancerRefreshesOnMemberUpdate) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91")};
  Upstream::HostMap all_hosts{{hosts[0]->address()->asString(), hosts[0]},
                              {hosts[1]->address()->asString(), hosts[1]}};
  init();

  // Install the initial slot assignment.
  factory_->onClusterSlotUpdate(std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
                                    ClusterSlot(0, 1000, hosts[0]->address()),
                                    ClusterSlot(1001, 16383, hosts[1]->address())}),
                                all_hosts);

  // Create a single LB *before* any further slot updates and exercise it.
  Upstream::LoadBalancerPtr lb = lb_->factory()->create(lb_params_);
  {
    TestLoadBalancerContext context(100); // slot 100 → hosts[0]
    EXPECT_EQ(hosts[0]->address()->asString(),
              lb->chooseHost(&context).host->address()->asString());
    TestLoadBalancerContext context2(2100); // slot 2100 → hosts[1]
    EXPECT_EQ(hosts[1]->address()->asString(),
              lb->chooseHost(&context2).host->address()->asString());
  }

  // Update slot assignment in the factory so that slot 2100 now maps to hosts[0].
  factory_->onClusterSlotUpdate(
      std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
          ClusterSlot(0, 1000, hosts[0]->address()), ClusterSlot(1001, 2000, hosts[1]->address()),
          ClusterSlot(2001, 16383, hosts[0]->address())}),
      all_hosts);

  // Until the worker priority set fires its member update callback, the LB
  // still holds the previous snapshot.
  {
    TestLoadBalancerContext context(2100);
    EXPECT_EQ(hosts[1]->address()->asString(),
              lb->chooseHost(&context).host->address()->asString());
  }

  // Simulate the worker host update broadcast: the cluster manager calls
  // priority_set_.updateHosts on the worker, which fires every registered
  // MemberUpdateCb -- including the one the LB installed in its constructor.
  worker_priority_set_.runUpdateCallbacks(0, {}, {});

  // The same LB instance now picks the updated assignment.
  {
    TestLoadBalancerContext context(2100);
    EXPECT_EQ(hosts[0]->address()->asString(),
              lb->chooseHost(&context).host->address()->asString());
  }
}

TEST_F(RedisClusterLoadBalancerTest, ClusterSlotNoUpdate) {
  Upstream::HostVector hosts{Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:92"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:91"),
                             Upstream::makeTestHost(info_, "tcp://127.0.0.1:92")};
  Upstream::HostVector replicas{Upstream::makeTestHost(info_, "tcp://127.0.0.2:90"),
                                Upstream::makeTestHost(info_, "tcp://127.0.0.2:91"),
                                Upstream::makeTestHost(info_, "tcp://127.0.0.2:90"),
                                Upstream::makeTestHost(info_, "tcp://127.0.0.2:91")};

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 1000, hosts[0]->address()),
      ClusterSlot(1001, 2000, hosts[1]->address()),
      ClusterSlot(2001, 16383, hosts[2]->address()),
  });

  (*slots)[0].addReplica(replicas[0]->address());
  (*slots)[0].addReplica(replicas[1]->address());
  Upstream::HostMap all_hosts{
      {hosts[0]->address()->asString(), hosts[0]},
      {hosts[1]->address()->asString(), hosts[1]},
      {hosts[2]->address()->asString(), hosts[2]},
      {replicas[0]->address()->asString(), replicas[0]},
      {replicas[1]->address()->asString(), replicas[1]},
  };

  // A list of (hash: host_index) pair.
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {100, 0}, {1100, 1}, {2100, 2}};

  init();
  EXPECT_EQ(true, factory_->onClusterSlotUpdate(std::move(slots), all_hosts));
  validateAssignment(hosts, expected_assignments);

  // Calling cluster slot update without change should not change assignment.
  std::vector<ClusterSlot> updated_slot{
      ClusterSlot(0, 1000, hosts[3]->address()),
      ClusterSlot(1001, 2000, hosts[4]->address()),
      ClusterSlot(2001, 16383, hosts[5]->address()),
  };
  updated_slot[0].addReplica(replicas[3]->address());
  updated_slot[0].addReplica(replicas[2]->address());
  EXPECT_EQ(false, factory_->onClusterSlotUpdate(
                       std::make_unique<std::vector<ClusterSlot>>(updated_slot), all_hosts));
  validateAssignment(hosts, expected_assignments);
}

TEST_F(RedisLoadBalancerContextImplTest, Basic) {
  // Simple read command
  std::vector<NetworkFilters::Common::Redis::RespValue> get_foo(2);
  get_foo[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[0].asString() = "get";
  get_foo[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[1].asString() = "foo";

  NetworkFilters::Common::Redis::RespValue get_request;
  get_request.type(NetworkFilters::Common::Redis::RespType::Array);
  get_request.asArray().swap(get_foo);

  RedisLoadBalancerContextImpl context1("foo", true, true, get_request,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context1.computeHashKey());
  EXPECT_EQ(true, context1.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context1.readPolicy());

  // Simple write command
  std::vector<NetworkFilters::Common::Redis::RespValue> set_foo(3);
  set_foo[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[0].asString() = "set";
  set_foo[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[1].asString() = "foo";
  set_foo[2].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[2].asString() = "bar";

  NetworkFilters::Common::Redis::RespValue set_request;
  set_request.type(NetworkFilters::Common::Redis::RespType::Array);
  set_request.asArray().swap(set_foo);

  RedisLoadBalancerContextImpl context2("foo", true, true, set_request,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context2.computeHashKey());
  EXPECT_EQ(false, context2.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context2.readPolicy());
}

TEST_F(RedisLoadBalancerContextImplTest, CompositeArray) {

  NetworkFilters::Common::Redis::RespValueSharedPtr base =
      std::make_shared<NetworkFilters::Common::Redis::RespValue>();
  makeBulkStringArray(*base, {"get", "foo", "bar"});

  // Composite read command
  NetworkFilters::Common::Redis::RespValue get_command;
  get_command.type(NetworkFilters::Common::Redis::RespType::SimpleString);
  get_command.asString() = "get";

  NetworkFilters::Common::Redis::RespValue get_request1{base, get_command, 1, 1};
  NetworkFilters::Common::Redis::RespValue get_request2{base, get_command, 2, 2};

  RedisLoadBalancerContextImpl context1("foo", true, true, get_request1,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context1.computeHashKey());
  EXPECT_EQ(true, context1.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context1.readPolicy());

  RedisLoadBalancerContextImpl context2("bar", true, true, get_request2,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(37829), context2.computeHashKey());
  EXPECT_EQ(true, context2.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context2.readPolicy());

  // Composite write command
  NetworkFilters::Common::Redis::RespValue set_command;
  set_command.type(NetworkFilters::Common::Redis::RespType::SimpleString);
  set_command.asString() = "set";

  NetworkFilters::Common::Redis::RespValue set_request{base, set_command, 1, 2};
  RedisLoadBalancerContextImpl context3("foo", true, true, set_request,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context3.computeHashKey());
  EXPECT_EQ(false, context3.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context3.readPolicy());
}

TEST_F(RedisLoadBalancerContextImplTest, UpperCaseCommand) {
  // Simple read command
  std::vector<NetworkFilters::Common::Redis::RespValue> get_foo(2);
  get_foo[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[0].asString() = "GET";
  get_foo[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[1].asString() = "foo";

  NetworkFilters::Common::Redis::RespValue get_request;
  get_request.type(NetworkFilters::Common::Redis::RespType::Array);
  get_request.asArray().swap(get_foo);

  RedisLoadBalancerContextImpl context1("foo", true, true, get_request,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context1.computeHashKey());
  EXPECT_EQ(true, context1.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context1.readPolicy());

  // Simple write command
  std::vector<NetworkFilters::Common::Redis::RespValue> set_foo(3);
  set_foo[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[0].asString() = "SET";
  set_foo[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[1].asString() = "foo";
  set_foo[2].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[2].asString() = "bar";

  NetworkFilters::Common::Redis::RespValue set_request;
  set_request.type(NetworkFilters::Common::Redis::RespType::Array);
  set_request.asArray().swap(set_foo);

  RedisLoadBalancerContextImpl context2("foo", true, true, set_request,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context2.computeHashKey());
  EXPECT_EQ(false, context2.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context2.readPolicy());
}

TEST_F(RedisLoadBalancerContextImplTest, UnsupportedCommand) {
  std::vector<NetworkFilters::Common::Redis::RespValue> unknown(1);
  unknown[0].type(NetworkFilters::Common::Redis::RespType::Integer);
  unknown[0].asInteger() = 1;
  NetworkFilters::Common::Redis::RespValue unknown_request;
  unknown_request.type(NetworkFilters::Common::Redis::RespType::Array);
  unknown_request.asArray().swap(unknown);

  RedisLoadBalancerContextImpl context3("foo", true, true, unknown_request,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context3.computeHashKey());
  EXPECT_EQ(false, context3.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context3.readPolicy());
}

TEST_F(RedisLoadBalancerContextImplTest, EnforceHashTag) {
  std::vector<NetworkFilters::Common::Redis::RespValue> set_foo(3);
  set_foo[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[0].asString() = "set";
  set_foo[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[1].asString() = "{foo}bar";
  set_foo[2].type(NetworkFilters::Common::Redis::RespType::BulkString);
  set_foo[2].asString() = "bar";

  NetworkFilters::Common::Redis::RespValue set_request;
  set_request.type(NetworkFilters::Common::Redis::RespType::Array);
  set_request.asArray().swap(set_foo);

  // Enable_hash tagging should be override when is_redis_cluster is true. This is treated like
  // "foo"
  RedisLoadBalancerContextImpl context2("{foo}bar", false, true, set_request,
                                        NetworkFilters::Common::Redis::Client::ReadPolicy::Primary);

  EXPECT_EQ(std::optional<uint64_t>(44950), context2.computeHashKey());
  EXPECT_EQ(false, context2.isReadCommand());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::Primary, context2.readPolicy());
}

TEST_F(RedisLoadBalancerContextImplTest, ClientZone) {
  std::vector<NetworkFilters::Common::Redis::RespValue> get_foo(2);
  get_foo[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[0].asString() = "get";
  get_foo[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  get_foo[1].asString() = "foo";

  NetworkFilters::Common::Redis::RespValue get_request;
  get_request.type(NetworkFilters::Common::Redis::RespType::Array);
  get_request.asArray().swap(get_foo);

  // Test with client zone specified
  RedisLoadBalancerContextImpl context1(
      "foo", true, true, get_request,
      NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinity, "us-east-1a");

  EXPECT_EQ("us-east-1a", context1.clientZone());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinity,
            context1.readPolicy());

  // Test with empty client zone
  RedisLoadBalancerContextImpl context2(
      "foo", true, true, get_request,
      NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityReplicasAndPrimary);

  EXPECT_EQ("", context2.clientZone());
  EXPECT_EQ(NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityReplicasAndPrimary,
            context2.readPolicy());
}

// Tests for LOCAL_ZONE_AFFINITY read policy with replicas in the same zone
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityWithLocalReplica) {
  // Setup: primary in zone-a, replica in zone-a (same as client)
  // Hosts must have locality zones set - RedisShard reads zone from host->locality().zone()
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary, zone-a
      makeHostWithZone("tcp://127.0.0.1:91", "zone-b"), // primary, zone-b
      makeHostWithZone("tcp://127.0.0.2:90", "zone-a"), // replica for slot 0, zone-a
      makeHostWithZone("tcp://127.0.0.2:91", "zone-b"), // replica for slot 1, zone-b
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 8000, hosts[0]->address()),
      ClusterSlot(8001, 16383, hosts[1]->address()),
  });
  slots->at(0).addReplica(hosts[2]->address());
  slots->at(1).addReplica(hosts[3]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Client is in zone-a, should prefer replica in zone-a for slot 0
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 2},    // slot 0: replica in zone-a
      {8001, 3}, // slot 1: replica in zone-b (no local replica, fall back to any replica)
  };
  validateAssignment(hosts, expected_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinity,
                     "zone-a");
}

// Tests for LOCAL_ZONE_AFFINITY when no local replica exists - should fall back to any replica
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityNoLocalReplica) {
  // Hosts must have locality zones set - RedisShard reads zone from host->locality().zone()
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary, zone-a
      makeHostWithZone("tcp://127.0.0.2:90", "zone-b"), // replica, zone-b
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });
  slots->at(0).addReplica(hosts[1]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Client is in zone-c (no local replica), should fall back to any replica
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 1}, // falls back to replica in zone-b
  };
  validateAssignment(hosts, expected_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinity,
                     "zone-c");
}

// Tests for LOCAL_ZONE_AFFINITY when no replica exists - should fall back to primary
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityNoReplica) {
  // Hosts must have locality zones set - RedisShard reads zone from host->locality().zone()
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary only
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // No replicas, should fall back to primary
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 0}, // falls back to primary
  };
  validateAssignment(hosts, expected_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinity,
                     "zone-a");
}

// Tests for LOCAL_ZONE_AFFINITY_REPLICAS_AND_PRIMARY with local replica
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityReplicasAndPrimaryWithLocalReplica) {
  // Hosts must have locality zones set - RedisShard reads zone from host->locality().zone()
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary, zone-a
      makeHostWithZone("tcp://127.0.0.2:90", "zone-a"), // replica, zone-a (same as client)
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });
  slots->at(0).addReplica(hosts[1]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Client is in zone-a, should prefer replica in zone-a
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 1}, // replica in zone-a
  };
  validateAssignment(
      hosts, expected_assignments, true,
      NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityReplicasAndPrimary,
      "zone-a");
}

// Tests for LOCAL_ZONE_AFFINITY_REPLICAS_AND_PRIMARY - prefer local primary when no local replica
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityReplicasAndPrimaryLocalPrimary) {
  // Hosts must have locality zones set - RedisShard reads zone from host->locality().zone()
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary, zone-a (same as client)
      makeHostWithZone("tcp://127.0.0.2:90", "zone-b"), // replica, zone-b
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });
  slots->at(0).addReplica(hosts[1]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Client is in zone-a, no local replica, but primary is in zone-a
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 0}, // primary in zone-a (no local replica, but local primary)
  };
  validateAssignment(
      hosts, expected_assignments, true,
      NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityReplicasAndPrimary,
      "zone-a");
}

// Tests for LOCAL_ZONE_AFFINITY_REPLICAS_AND_PRIMARY - fall back to any replica when no local hosts
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityReplicasAndPrimaryFallbackToReplica) {
  // Hosts must have locality zones set - RedisShard reads zone from host->locality().zone()
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary, zone-a
      makeHostWithZone("tcp://127.0.0.2:90", "zone-b"), // replica, zone-b
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });
  slots->at(0).addReplica(hosts[1]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Client is in zone-c (no local hosts), should fall back to any replica
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 1}, // falls back to replica
  };
  validateAssignment(
      hosts, expected_assignments, true,
      NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityReplicasAndPrimary,
      "zone-c");
}

// Tests for LOCAL_ZONE_AFFINITY_REPLICAS_AND_PRIMARY - fall back to primary when no replicas
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityReplicasAndPrimaryNoReplica) {
  // Hosts must have locality zones set - RedisShard reads zone from host->locality().zone()
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary only, zone-a
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // No replicas, should fall back to primary
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 0}, // falls back to primary
  };
  validateAssignment(
      hosts, expected_assignments, true,
      NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityReplicasAndPrimary,
      "zone-c");
}

// Tests for LOCAL_ZONE_AFFINITY when zone discovery fails - hosts have no zones set
// Should fall back to any-replica behavior (same as PreferReplica)
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityZoneDiscoveryFailure) {
  // Hosts without zone set - simulates zone discovery failure scenario
  Upstream::HostVector hosts{
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"), // primary, no zone
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:90"), // replica, no zone
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });
  slots->at(0).addReplica(hosts[1]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Client has a zone, but hosts don't - should fall back to any replica
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 1}, // falls back to any replica since no host zones match
  };
  validateAssignment(hosts, expected_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinity,
                     "zone-a");
}

// Tests for LOCAL_ZONE_AFFINITY_REPLICAS_AND_PRIMARY - unhealthy local primary falls through
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityReplicasAndPrimaryUnhealthyLocalPrimary) {
  Upstream::HostVector hosts{
      makeHostWithZone("tcp://127.0.0.1:90", "zone-a"), // primary, zone-a (same as client)
      makeHostWithZone("tcp://127.0.0.2:90", "zone-b"), // replica, zone-b
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });
  slots->at(0).addReplica(hosts[1]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Mark primary unhealthy - should skip local primary even though zone matches
  hosts[0]->healthFlagSet(Upstream::Host::HealthFlag::FAILED_ACTIVE_HC);
  factory_->onHostHealthUpdate();

  // Client is in zone-a, local primary is unhealthy, no local replica → fall back to any replica
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 1}, // falls back to replica in zone-b
  };
  validateAssignment(
      hosts, expected_assignments, true,
      NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityReplicasAndPrimary,
      "zone-a");
}

// Tests for LOCAL_ZONE_AFFINITY with empty client zone - should behave like PreferReplica
TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityEmptyClientZone) {
  Upstream::HostVector hosts{
      Upstream::makeTestHost(info_, "tcp://127.0.0.1:90"), // primary
      Upstream::makeTestHost(info_, "tcp://127.0.0.2:90"), // replica
  };

  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(std::vector<ClusterSlot>{
      ClusterSlot(0, 16383, hosts[0]->address()),
  });
  slots->at(0).addReplica(hosts[1]->address());

  Upstream::HostMap all_hosts;
  std::transform(hosts.begin(), hosts.end(), std::inserter(all_hosts, all_hosts.end()), makePair);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);

  // Empty client zone, should fall back to any replica
  const std::vector<std::pair<uint32_t, uint32_t>> expected_assignments = {
      {0, 1}, // any replica
  };
  validateAssignment(hosts, expected_assignments, true,
                     NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinity, "");
}

struct LocalZoneAffinityAnyTestCase {
  std::string name;
  std::vector<std::string> zones;
  std::vector<std::optional<Upstream::Host::HealthFlag>> health_flags;
  std::string client_zone;
  std::vector<uint32_t> expected_hosts;
};

class LocalZoneAffinityAnyTest : public RedisClusterLoadBalancerTest,
                                 public testing::WithParamInterface<LocalZoneAffinityAnyTestCase> {
};

TEST_P(LocalZoneAffinityAnyTest, Selection) {
  const auto& test_case = GetParam();
  Upstream::HostVector hosts;
  for (uint32_t i = 0; i < test_case.zones.size(); ++i) {
    auto host = makeHostWithZone("tcp://127.0.0.1:" + std::to_string(90 + i), test_case.zones[i]);
    if (i < test_case.health_flags.size() && test_case.health_flags[i].has_value()) {
      host->healthFlagSet(test_case.health_flags[i].value());
    }
    hosts.push_back(host);
  }
  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(
      std::vector<ClusterSlot>{ClusterSlot(0, 8191, hosts[0]->address())});
  for (uint32_t i = 1; i < hosts.size(); ++i) {
    slots->at(0).addReplica(hosts[i]->address());
  }
  auto other_primary = makeHostWithZone("tcp://127.0.0.2:90", "zone-a");
  slots->emplace_back(8192, 16383, other_primary->address());
  auto all_hosts = generateHostMap(hosts);
  all_hosts.emplace(other_primary->address()->asString(), other_primary);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);
  auto lb = lb_->factory()->create(lb_params_);

  NetworkFilters::Common::Redis::RespValue request;
  request.type(NetworkFilters::Common::Redis::RespType::Array);
  request.asArray().resize(2);
  request.asArray()[0].type(NetworkFilters::Common::Redis::RespType::BulkString);
  request.asArray()[0].asString() = "get";
  request.asArray()[1].type(NetworkFilters::Common::Redis::RespType::BulkString);
  request.asArray()[1].asString() = "foo";
  const auto policy = NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityAny;
  TestLoadBalancerContext key_context(100, true, policy, test_case.client_zone);
  RedisSpecifyShardContextImpl shard_context(0, request, policy, test_case.client_zone);

  // Shard construction also uses the RNG; only constrain draws after installing the topology.
  // Replicas are sorted by address, so these indexes follow the host vector.
  for (uint64_t i = 0; i < test_case.expected_hosts.size() * 2; ++i) {
    EXPECT_CALL(random_, random()).Times(2).WillRepeatedly(Return(i));
    const auto& expected = hosts[test_case.expected_hosts[i % test_case.expected_hosts.size()]];
    EXPECT_EQ(expected, lb->chooseHost(&key_context).host);
    EXPECT_EQ(expected, lb->chooseHost(&shard_context).host);
  }

  TestLoadBalancerContext other_key(8192, true, policy, test_case.client_zone);
  RedisSpecifyShardContextImpl other_shard(1, request, policy, test_case.client_zone);
  EXPECT_CALL(random_, random()).Times(2).WillRepeatedly(Return(17));
  EXPECT_EQ(other_primary, lb->chooseHost(&other_key).host);
  EXPECT_EQ(other_primary, lb->chooseHost(&other_shard).host);

  EXPECT_CALL(random_, random()).Times(0);
  TestLoadBalancerContext write_context(100, false, policy, test_case.client_zone);
  EXPECT_EQ(hosts[0], lb->chooseHost(&write_context).host);
  RedisSpecifyShardContextImpl invalid_shard(2, request, policy, test_case.client_zone);
  EXPECT_EQ(nullptr, lb->chooseHost(&invalid_shard).host);
}

using HealthFlag = Upstream::Host::HealthFlag;
INSTANTIATE_TEST_SUITE_P(
    ReadPolicies, LocalZoneAffinityAnyTest,
    testing::Values(
        LocalZoneAffinityAnyTestCase{"LocalPrimaryAndReplicas",
                                     {"zone-a", "zone-a", "zone-a", "zone-b", ""},
                                     {},
                                     "zone-a",
                                     {0, 1, 2}},
        LocalZoneAffinityAnyTestCase{"LocalPrimaryOnly", {"zone-a", "zone-b"}, {}, "zone-a", {0}},
        LocalZoneAffinityAnyTestCase{
            "LocalReplicasOnly", {"zone-b", "zone-a", "zone-a"}, {}, "zone-a", {1, 2}},
        LocalZoneAffinityAnyTestCase{"NoReplicas", {"zone-b"}, {}, "zone-a", {0}},
        LocalZoneAffinityAnyTestCase{"FailedLocalMembers",
                                     {"zone-a", "zone-a", "zone-b", ""},
                                     {HealthFlag::FAILED_ACTIVE_HC, HealthFlag::FAILED_ACTIVE_HC},
                                     "zone-a",
                                     {2, 3}},
        LocalZoneAffinityAnyTestCase{"OutlierFailedLocal",
                                     {"zone-a", "zone-b"},
                                     {HealthFlag::FAILED_OUTLIER_CHECK},
                                     "zone-a",
                                     {1}},
        LocalZoneAffinityAnyTestCase{
            "DegradedLocal", {"zone-a", "zone-b"}, {HealthFlag::DEGRADED_ACTIVE_HC}, "zone-a", {1}},
        LocalZoneAffinityAnyTestCase{
            "EmptyClientZone", {"zone-a", "zone-b", ""}, {}, "", {0, 1, 2}},
        LocalZoneAffinityAnyTestCase{
            "UnmatchedClientZone", {"zone-a", "zone-b", ""}, {}, "zone-c", {0, 1, 2}},
        LocalZoneAffinityAnyTestCase{"MissingBackendZones", {"", "", ""}, {}, "zone-a", {0, 1, 2}},
        LocalZoneAffinityAnyTestCase{
            "ExactZoneMatch", {"zone-a", "ZONE-A", "zone-a "}, {}, "zone-a", {0}},
        LocalZoneAffinityAnyTestCase{"MixedDegradedAndFailed",
                                     {"zone-a", "zone-b"},
                                     {HealthFlag::DEGRADED_ACTIVE_HC, HealthFlag::FAILED_ACTIVE_HC},
                                     "zone-a",
                                     {0, 1}},
        LocalZoneAffinityAnyTestCase{"AllFailed",
                                     {"zone-a", "zone-b"},
                                     {HealthFlag::FAILED_ACTIVE_HC, HealthFlag::FAILED_ACTIVE_HC},
                                     "zone-a",
                                     {0, 1}},
        LocalZoneAffinityAnyTestCase{
            "AllDegraded",
            {"zone-a", "zone-b"},
            {HealthFlag::DEGRADED_EDS_HEALTH, HealthFlag::DEGRADED_EDS_HEALTH},
            "zone-a",
            {0, 1}},
        LocalZoneAffinityAnyTestCase{
            "AllDraining",
            {"zone-a", "zone-b"},
            {HealthFlag::EDS_STATUS_DRAINING, HealthFlag::EDS_STATUS_DRAINING},
            "zone-a",
            {0, 1}}),
    [](const testing::TestParamInfo<LocalZoneAffinityAnyTestCase>& info) {
      return info.param.name;
    });

TEST_F(RedisClusterLoadBalancerTest, LocalZoneAffinityAnyHealthUpdates) {
  Upstream::HostVector hosts{makeHostWithZone("tcp://127.0.0.1:90", "zone-a"),
                             makeHostWithZone("tcp://127.0.0.1:91", "zone-a"),
                             makeHostWithZone("tcp://127.0.0.1:92", "zone-b")};
  ClusterSlotsPtr slots = std::make_unique<std::vector<ClusterSlot>>(
      std::vector<ClusterSlot>{ClusterSlot(0, 16383, hosts[0]->address())});
  slots->at(0).addReplica(hosts[1]->address());
  slots->at(0).addReplica(hosts[2]->address());
  auto all_hosts = generateHostMap(hosts);
  init();
  factory_->onClusterSlotUpdate(std::move(slots), all_hosts);
  auto lb = lb_->factory()->create(lb_params_);
  TestLoadBalancerContext context(
      100, true, NetworkFilters::Common::Redis::Client::ReadPolicy::LocalZoneAffinityAny, "zone-a");

  EXPECT_CALL(random_, random()).WillOnce(Return(1));
  EXPECT_EQ(hosts[1], lb->chooseHost(&context).host);
  testing::Mock::VerifyAndClearExpectations(&random_);

  hosts[0]->healthFlagSet(HealthFlag::FAILED_ACTIVE_HC);
  hosts[1]->healthFlagSet(HealthFlag::FAILED_ACTIVE_HC);
  factory_->onHostHealthUpdate();
  worker_priority_set_.runUpdateCallbacks(0, {}, {});
  EXPECT_CALL(random_, random()).WillOnce(Return(0));
  EXPECT_EQ(hosts[2], lb->chooseHost(&context).host);
  testing::Mock::VerifyAndClearExpectations(&random_);

  hosts[0]->healthFlagClear(HealthFlag::FAILED_ACTIVE_HC);
  factory_->onHostHealthUpdate();
  worker_priority_set_.runUpdateCallbacks(0, {}, {});
  EXPECT_CALL(random_, random()).WillOnce(Return(1));
  EXPECT_EQ(hosts[0], lb->chooseHost(&context).host);
  testing::Mock::VerifyAndClearExpectations(&random_);

  hosts[0]->healthFlagSet(HealthFlag::FAILED_ACTIVE_HC);
  hosts[2]->healthFlagSet(HealthFlag::FAILED_ACTIVE_HC);
  factory_->onHostHealthUpdate();
  worker_priority_set_.runUpdateCallbacks(0, {}, {});
  EXPECT_CALL(random_, random()).WillOnce(Return(1));
  EXPECT_EQ(hosts[1], lb->chooseHost(&context).host);
}

} // namespace Redis
} // namespace Clusters
} // namespace Extensions
} // namespace Envoy
