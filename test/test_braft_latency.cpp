// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Tests for PR #3: BRAFT_LATENCY macros, MetaPeriodicSyncTimer,
// raft_meta force_no_sync / periodic_sync / pre-serialization features.

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <butil/time.h>
#include <butil/status.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include "braft/raft.h"
#include "braft/raft_meta.h"
#include "braft/braft_latency.h"

namespace braft {
extern void global_init_once_or_die();
}

DECLARE_bool(raft_meta_force_no_sync);
DECLARE_bool(raft_meta_periodic_sync_enabled);
DECLARE_int32(raft_meta_periodic_sync_interval_ms);
DECLARE_bool(raft_meta_enable_preserialize);
DECLARE_bool(raft_meta_enable_leveldb_tuning);
DECLARE_bool(raft_sync_meta);

// ============================================================
// BRAFT_LATENCY macro tests
// ============================================================

class BraftLatencyTest : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(BraftLatencyTest, disabled_has_zero_overhead) {
    // When flag is false, start_ts should remain 0 (no time call made)
    BRAFT_LATENCY_BEGIN(false)
    ASSERT_EQ(0, braft_latency_start_ts);
    ASSERT_FALSE(braft_latency_is_enabled);

    BRAFT_LATENCY_CONTINUE(checkpoint1)
    ASSERT_EQ(0, checkpoint1);

    // BRAFT_LATENCY_END should not execute the log body
    BRAFT_LATENCY_END(0, "should not reach here: " << braft_latency_elapsed)
}

TEST_F(BraftLatencyTest, enabled_captures_timestamps) {
    BRAFT_LATENCY_BEGIN(true)
    ASSERT_TRUE(braft_latency_is_enabled);
    ASSERT_GT(braft_latency_start_ts, 0);

    usleep(5000);  // 5ms

    BRAFT_LATENCY_CONTINUE(mid_point)
    ASSERT_GT(mid_point, 0);
    ASSERT_GE(mid_point, braft_latency_start_ts);

    usleep(5000);  // another 5ms

    // Use a high threshold so LOG_IF doesn't fire
    BRAFT_LATENCY_END(999999, "test: elapsed=" << braft_latency_elapsed
                      << " mid_delta=" << (mid_point - braft_latency_start_ts))
}

TEST_F(BraftLatencyTest, multiple_continue_checkpoints) {
    BRAFT_LATENCY_BEGIN(true)

    BRAFT_LATENCY_CONTINUE(cp1)
    usleep(2000);
    BRAFT_LATENCY_CONTINUE(cp2)
    usleep(2000);
    BRAFT_LATENCY_CONTINUE(cp3)

    // Timestamps should be monotonically non-decreasing
    ASSERT_GE(cp1, braft_latency_start_ts);
    ASSERT_GE(cp2, cp1);
    ASSERT_GE(cp3, cp2);

    BRAFT_LATENCY_END(999999, "test")
}

TEST_F(BraftLatencyTest, threshold_flag_default) {
    // Verify the gflag exists and has a reasonable default
    ASSERT_GT(FLAGS_raft_latency_log_threshold_ms, 0);
    ASSERT_EQ(1000, FLAGS_raft_latency_log_threshold_ms);
}

// ============================================================
// MetaPeriodicSyncTimer tests
// ============================================================

class MetaPeriodicSyncTimerTest : public testing::Test {
protected:
    void SetUp() {
        braft::global_init_once_or_die();
        _db_path = "./test_periodic_sync_db";
        system("rm -rf ./test_periodic_sync_db");

        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status st = leveldb::DB::Open(options, _db_path, &_db);
        ASSERT_TRUE(st.ok()) << st.ToString();
    }
    void TearDown() {
        delete _db;
        _db = nullptr;
        system("rm -rf ./test_periodic_sync_db");
    }

    leveldb::DB* _db = nullptr;
    std::string _db_path;
};

TEST_F(MetaPeriodicSyncTimerTest, init_and_destroy) {
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(100));  // 100ms interval
    timer.start();
    usleep(50000);  // 50ms, let timer tick
    timer.stop();
    timer.destroy();
    timer.wait_for_destroy();
}

TEST_F(MetaPeriodicSyncTimerTest, dirty_flag_triggers_sync) {
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(50));  // 50ms interval

    // Initially not dirty — last_sync_time should be 0
    ASSERT_EQ(0, timer.last_sync_time_ms());

    timer.start();
    // Wait a tick without marking dirty — should NOT sync
    usleep(100000);  // 100ms
    ASSERT_EQ(0, timer.last_sync_time_ms());

    // Mark dirty and wait for sync
    timer.mark_dirty();
    usleep(150000);  // 150ms — enough for at least one tick

    // After sync, last_sync_time should be set
    ASSERT_GT(timer.last_sync_time_ms(), 0);

    timer.stop();
    timer.destroy();
    timer.wait_for_destroy();
}

TEST_F(MetaPeriodicSyncTimerTest, not_dirty_skips_sync) {
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(50));  // 50ms

    timer.start();
    // Don't mark dirty, wait several ticks
    usleep(200000);  // 200ms

    // Should never have synced
    ASSERT_EQ(0, timer.last_sync_time_ms());

    timer.stop();
    timer.destroy();
    timer.wait_for_destroy();
}

TEST_F(MetaPeriodicSyncTimerTest, multiple_dirty_marks) {
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(50));

    timer.start();

    // Mark dirty multiple times rapidly
    timer.mark_dirty();
    timer.mark_dirty();
    timer.mark_dirty();
    usleep(150000);

    int64_t first_sync = timer.last_sync_time_ms();
    ASSERT_GT(first_sync, 0);

    // Mark dirty again and wait for another sync
    timer.mark_dirty();
    usleep(150000);

    int64_t second_sync = timer.last_sync_time_ms();
    ASSERT_GE(second_sync, first_sync);

    timer.stop();
    timer.destroy();
    timer.wait_for_destroy();
}

// ============================================================
// KVBasedMergedMetaStorage with new flags
// ============================================================

class MetaStorageFlagsTest : public testing::Test {
protected:
    void SetUp() {
        braft::global_init_once_or_die();
        system("rm -rf ./test_meta_flags_db");
        // Save original flag values
        _orig_force_no_sync = FLAGS_raft_meta_force_no_sync;
        _orig_periodic_sync = FLAGS_raft_meta_periodic_sync_enabled;
        _orig_periodic_interval = FLAGS_raft_meta_periodic_sync_interval_ms;
        _orig_preserialize = FLAGS_raft_meta_enable_preserialize;
        _orig_leveldb_tuning = FLAGS_raft_meta_enable_leveldb_tuning;
        _orig_sync_meta = FLAGS_raft_sync_meta;
    }

    void TearDown() {
        // Restore flags
        FLAGS_raft_meta_force_no_sync = _orig_force_no_sync;
        FLAGS_raft_meta_periodic_sync_enabled = _orig_periodic_sync;
        FLAGS_raft_meta_periodic_sync_interval_ms = _orig_periodic_interval;
        FLAGS_raft_meta_enable_preserialize = _orig_preserialize;
        FLAGS_raft_meta_enable_leveldb_tuning = _orig_leveldb_tuning;
        FLAGS_raft_sync_meta = _orig_sync_meta;
        system("rm -rf ./test_meta_flags_db");
    }

    bool _orig_force_no_sync;
    bool _orig_periodic_sync;
    int32_t _orig_periodic_interval;
    bool _orig_preserialize;
    bool _orig_leveldb_tuning;
    bool _orig_sync_meta;
};

TEST_F(MetaStorageFlagsTest, basic_write_read_with_default_flags) {
    // Default flags: no force_no_sync, no periodic sync, no preserialize
    braft::KVBasedMergedMetaStorage* storage =
        new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());

    std::string vgid = "test_group_0";
    braft::PeerId candidate;
    ASSERT_EQ(0, candidate.parse("1.1.1.1:1000:0"));
    ASSERT_TRUE(storage->set_term_and_votedfor(10, candidate, vgid).ok());

    int64_t term_bak = 0;
    braft::PeerId peer_bak;
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid).ok());
    ASSERT_EQ(10, term_bak);
    ASSERT_EQ(candidate.addr, peer_bak.addr);

    delete storage;
}

TEST_F(MetaStorageFlagsTest, force_no_sync_write_read) {
    FLAGS_raft_meta_force_no_sync = true;
    FLAGS_raft_sync_meta = true;  // Would normally sync, but force_no_sync overrides

    braft::KVBasedMergedMetaStorage* storage =
        new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());

    std::string vgid = "test_group_0";
    braft::PeerId candidate;
    ASSERT_EQ(0, candidate.parse("1.1.1.1:1000:0"));
    ASSERT_TRUE(storage->set_term_and_votedfor(10, candidate, vgid).ok());

    int64_t term_bak = 0;
    braft::PeerId peer_bak;
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid).ok());
    ASSERT_EQ(10, term_bak);
    ASSERT_EQ(candidate.addr, peer_bak.addr);
    delete storage;

    // Data should survive process "restart" (reload from disk)
    FLAGS_raft_meta_force_no_sync = false;
    storage = new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());
    term_bak = 0;
    peer_bak.reset();
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid).ok());
    ASSERT_EQ(10, term_bak);
    ASSERT_EQ(candidate.addr, peer_bak.addr);
    delete storage;
}

TEST_F(MetaStorageFlagsTest, periodic_sync_write_read) {
    FLAGS_raft_meta_force_no_sync = true;
    FLAGS_raft_meta_periodic_sync_enabled = true;
    FLAGS_raft_meta_periodic_sync_interval_ms = 100;  // 100ms

    braft::KVBasedMergedMetaStorage* storage =
        new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());

    std::string vgid = "test_group_0";
    braft::PeerId candidate;
    ASSERT_EQ(0, candidate.parse("2.2.2.2:2000:0"));
    ASSERT_TRUE(storage->set_term_and_votedfor(20, candidate, vgid).ok());

    // Wait for periodic sync to fire
    usleep(300000);  // 300ms

    int64_t term_bak = 0;
    braft::PeerId peer_bak;
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid).ok());
    ASSERT_EQ(20, term_bak);
    ASSERT_EQ(candidate.addr, peer_bak.addr);
    delete storage;

    // Reload and verify persistence
    FLAGS_raft_meta_periodic_sync_enabled = false;
    FLAGS_raft_meta_force_no_sync = false;
    storage = new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());
    term_bak = 0;
    peer_bak.reset();
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid).ok());
    ASSERT_EQ(20, term_bak);
    ASSERT_EQ(candidate.addr, peer_bak.addr);
    delete storage;
}

TEST_F(MetaStorageFlagsTest, preserialize_consistency) {
    // Write data WITHOUT pre-serialization
    FLAGS_raft_meta_enable_preserialize = false;

    braft::KVBasedMergedMetaStorage* storage =
        new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());

    std::string vgid1 = "group_preser_0";
    std::string vgid2 = "group_preser_1";
    braft::PeerId peer1, peer2;
    ASSERT_EQ(0, peer1.parse("10.0.0.1:8000:0"));
    ASSERT_EQ(0, peer2.parse("10.0.0.2:9000:1"));

    ASSERT_TRUE(storage->set_term_and_votedfor(100, peer1, vgid1).ok());
    ASSERT_TRUE(storage->set_term_and_votedfor(200, peer2, vgid2).ok());
    delete storage;

    // Read back WITH pre-serialization enabled — data should match
    FLAGS_raft_meta_enable_preserialize = true;
    storage = new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());

    int64_t term_bak = 0;
    braft::PeerId peer_bak;
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid1).ok());
    ASSERT_EQ(100, term_bak);
    ASSERT_EQ(peer1.addr, peer_bak.addr);
    ASSERT_EQ(peer1.idx, peer_bak.idx);

    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid2).ok());
    ASSERT_EQ(200, term_bak);
    ASSERT_EQ(peer2.addr, peer_bak.addr);
    ASSERT_EQ(peer2.idx, peer_bak.idx);

    // Write with pre-serialization and verify
    std::string vgid3 = "group_preser_2";
    braft::PeerId peer3;
    ASSERT_EQ(0, peer3.parse("10.0.0.3:7000:2"));
    ASSERT_TRUE(storage->set_term_and_votedfor(300, peer3, vgid3).ok());
    delete storage;

    // Read back with pre-serialization OFF — should still be consistent
    FLAGS_raft_meta_enable_preserialize = false;
    storage = new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid3).ok());
    ASSERT_EQ(300, term_bak);
    ASSERT_EQ(peer3.addr, peer_bak.addr);
    ASSERT_EQ(peer3.idx, peer_bak.idx);
    delete storage;
}

TEST_F(MetaStorageFlagsTest, leveldb_tuning_init) {
    FLAGS_raft_meta_enable_leveldb_tuning = true;

    braft::KVBasedMergedMetaStorage* storage =
        new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());

    // Write and read to verify the tuned DB works correctly
    std::string vgid = "tuned_group_0";
    braft::PeerId candidate;
    ASSERT_EQ(0, candidate.parse("3.3.3.3:3000:0"));
    ASSERT_TRUE(storage->set_term_and_votedfor(30, candidate, vgid).ok());

    int64_t term_bak = 0;
    braft::PeerId peer_bak;
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid).ok());
    ASSERT_EQ(30, term_bak);
    ASSERT_EQ(candidate.addr, peer_bak.addr);
    delete storage;

    // Reload without tuning — data should still be readable
    FLAGS_raft_meta_enable_leveldb_tuning = false;
    storage = new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());
    term_bak = 0;
    peer_bak.reset();
    ASSERT_TRUE(storage->get_term_and_votedfor(&term_bak, &peer_bak, vgid).ok());
    ASSERT_EQ(30, term_bak);
    ASSERT_EQ(candidate.addr, peer_bak.addr);
    delete storage;
}

TEST_F(MetaStorageFlagsTest, multiple_groups_with_preserialize) {
    FLAGS_raft_meta_enable_preserialize = true;

    braft::KVBasedMergedMetaStorage* storage =
        new braft::KVBasedMergedMetaStorage("./test_meta_flags_db");
    ASSERT_TRUE(storage->init().ok());

    // Write multiple groups concurrently-ish
    const int num_groups = 50;
    for (int i = 0; i < num_groups; i++) {
        std::string vgid = "batch_group_" + std::to_string(i);
        braft::PeerId peer;
        std::string addr = "10.0.0." + std::to_string(i % 256) + ":"
                           + std::to_string(8000 + i) + ":0";
        ASSERT_EQ(0, peer.parse(addr));
        ASSERT_TRUE(storage->set_term_and_votedfor(
            1000 + i, peer, vgid).ok());
    }

    // Verify all groups
    for (int i = 0; i < num_groups; i++) {
        std::string vgid = "batch_group_" + std::to_string(i);
        int64_t term_bak = 0;
        braft::PeerId peer_bak;
        ASSERT_TRUE(storage->get_term_and_votedfor(
            &term_bak, &peer_bak, vgid).ok());
        ASSERT_EQ(1000 + i, term_bak);
    }
    delete storage;
}

// ============================================================
// Empty WriteBatch sync verification
// ============================================================

class EmptyWriteBatchSyncTest : public testing::Test {
protected:
    void SetUp() {
        _db_path = "./test_empty_wb_sync_db";
        system("rm -rf ./test_empty_wb_sync_db");

        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status st = leveldb::DB::Open(options, _db_path, &_db);
        ASSERT_TRUE(st.ok()) << st.ToString();
    }
    void TearDown() {
        delete _db;
        _db = nullptr;
        system("rm -rf ./test_empty_wb_sync_db");
    }

    leveldb::DB* _db = nullptr;
    std::string _db_path;
};

TEST_F(EmptyWriteBatchSyncTest, empty_batch_with_sync_succeeds) {
    // This test verifies the assumption that MetaPeriodicSyncTimer::run()
    // relies on: LevelDB accepts an empty WriteBatch with sync=true.
    leveldb::WriteOptions sync_options;
    sync_options.sync = true;
    leveldb::WriteBatch empty_batch;
    leveldb::Status st = _db->Write(sync_options, &empty_batch);
    ASSERT_TRUE(st.ok()) << "Empty WriteBatch with sync=true failed: "
                         << st.ToString();
}

TEST_F(EmptyWriteBatchSyncTest, data_survives_empty_sync_batch) {
    // Write real data without sync
    leveldb::WriteOptions no_sync;
    no_sync.sync = false;
    ASSERT_TRUE(_db->Put(no_sync, "key1", "value1").ok());

    // Sync with empty batch
    leveldb::WriteOptions sync_options;
    sync_options.sync = true;
    leveldb::WriteBatch empty_batch;
    ASSERT_TRUE(_db->Write(sync_options, &empty_batch).ok());

    // Verify data is still readable
    std::string value;
    ASSERT_TRUE(_db->Get(leveldb::ReadOptions(), "key1", &value).ok());
    ASSERT_EQ("value1", value);
}

// ============================================================
// gflags validator tests
// ============================================================

class GflagValidatorTest : public testing::Test {
protected:
    void SetUp() {
        braft::global_init_once_or_die();
        // Save original values
        _orig_leveldb_tuning = FLAGS_raft_meta_enable_leveldb_tuning;
        _orig_periodic_sync = FLAGS_raft_meta_periodic_sync_enabled;
        _orig_force_no_sync = FLAGS_raft_meta_force_no_sync;
        _orig_interval = FLAGS_raft_meta_periodic_sync_interval_ms;
    }
    void TearDown() {
        // Restore via direct assignment (bypasses validator, which is fine in tests)
        FLAGS_raft_meta_enable_leveldb_tuning = _orig_leveldb_tuning;
        FLAGS_raft_meta_periodic_sync_enabled = _orig_periodic_sync;
        FLAGS_raft_meta_force_no_sync = _orig_force_no_sync;
        FLAGS_raft_meta_periodic_sync_interval_ms = _orig_interval;
    }

    bool _orig_leveldb_tuning;
    bool _orig_periodic_sync;
    bool _orig_force_no_sync;
    int32_t _orig_interval;
};

TEST_F(GflagValidatorTest, leveldb_tuning_rejects_runtime_change) {
    // Default is false; trying to change to true at runtime should fail
    ASSERT_FALSE(FLAGS_raft_meta_enable_leveldb_tuning);
    std::string result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_enable_leveldb_tuning", "true");
    // SetCommandLineOption returns empty string on failure
    ASSERT_TRUE(result.empty())
        << "Validator should reject runtime change of leveldb_tuning";
    ASSERT_FALSE(FLAGS_raft_meta_enable_leveldb_tuning);

    // Setting to same value (false→false) should succeed (no-op)
    result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_enable_leveldb_tuning", "false");
    ASSERT_FALSE(result.empty())
        << "Validator should accept setting same value";
}

TEST_F(GflagValidatorTest, periodic_sync_enabled_rejects_runtime_change) {
    // Default is false; trying to change to true at runtime should fail
    ASSERT_FALSE(FLAGS_raft_meta_periodic_sync_enabled);
    std::string result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_periodic_sync_enabled", "true");
    ASSERT_TRUE(result.empty())
        << "Validator should reject runtime change of periodic_sync_enabled";
    ASSERT_FALSE(FLAGS_raft_meta_periodic_sync_enabled);
}

TEST_F(GflagValidatorTest, force_no_sync_allows_runtime_change) {
    // force_no_sync can be changed at runtime (validator always returns true)
    ASSERT_FALSE(FLAGS_raft_meta_force_no_sync);
    std::string result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_force_no_sync", "true");
    ASSERT_FALSE(result.empty())
        << "Validator should allow runtime change of force_no_sync";
    ASSERT_TRUE(FLAGS_raft_meta_force_no_sync);

    // Change back
    result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_force_no_sync", "false");
    ASSERT_FALSE(result.empty());
    ASSERT_FALSE(FLAGS_raft_meta_force_no_sync);
}

TEST_F(GflagValidatorTest, preserialize_allows_runtime_change) {
    // preserialize uses PassValidate, so runtime changes should work
    ASSERT_FALSE(FLAGS_raft_meta_enable_preserialize);
    std::string result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_enable_preserialize", "true");
    ASSERT_FALSE(result.empty())
        << "Validator should allow runtime change of preserialize";
    ASSERT_TRUE(FLAGS_raft_meta_enable_preserialize);
    // Restore
    FLAGS_raft_meta_enable_preserialize = false;
}

TEST_F(GflagValidatorTest, periodic_sync_interval_rejects_negative) {
    // Uses NonNegativeInteger validator
    int32_t old_val = FLAGS_raft_meta_periodic_sync_interval_ms;
    std::string result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_periodic_sync_interval_ms", "-1");
    ASSERT_TRUE(result.empty())
        << "Validator should reject negative interval";
    ASSERT_EQ(old_val, FLAGS_raft_meta_periodic_sync_interval_ms);

    // Positive value should work
    result = GFLAGS_NS::SetCommandLineOption(
        "raft_meta_periodic_sync_interval_ms", "500");
    ASSERT_FALSE(result.empty());
    ASSERT_EQ(500, FLAGS_raft_meta_periodic_sync_interval_ms);
}

TEST_F(GflagValidatorTest, latency_threshold_rejects_negative) {
    // raft_latency_log_threshold_ms uses NonNegativeInteger
    int32_t old_val = FLAGS_raft_latency_log_threshold_ms;
    std::string result = GFLAGS_NS::SetCommandLineOption(
        "raft_latency_log_threshold_ms", "-100");
    ASSERT_TRUE(result.empty())
        << "Validator should reject negative threshold";
    ASSERT_EQ(old_val, FLAGS_raft_latency_log_threshold_ms);

    // Zero should be accepted
    result = GFLAGS_NS::SetCommandLineOption(
        "raft_latency_log_threshold_ms", "0");
    ASSERT_FALSE(result.empty());
    ASSERT_EQ(0, FLAGS_raft_latency_log_threshold_ms);
    // Restore
    FLAGS_raft_latency_log_threshold_ms = old_val;
}

// ============================================================
// MetaPeriodicSyncTimer detailed behavior
// ============================================================

TEST_F(MetaPeriodicSyncTimerTest, direct_run_syncs_when_dirty) {
    // Test run() directly without using the timer scheduling.
    // Since test builds use -Dprivate=public, we can access internals.
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(1000));  // interval doesn't matter for direct call

    // Not dirty — run() should be a no-op
    ASSERT_EQ(0, timer.last_sync_time_ms());
    timer.run();
    ASSERT_EQ(0, timer.last_sync_time_ms());

    // Mark dirty and run — should sync
    timer.mark_dirty();
    ASSERT_TRUE(timer._dirty.load());
    timer.run();
    ASSERT_FALSE(timer._dirty.load());  // dirty cleared after successful sync
    ASSERT_GT(timer.last_sync_time_ms(), 0);

    timer.destroy();
    timer.wait_for_destroy();
}

TEST_F(MetaPeriodicSyncTimerTest, dirty_restored_concept) {
    // Verify the dirty flag pattern: after mark_dirty + successful run,
    // dirty is false. After another mark_dirty, it's true again.
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(1000));

    timer.mark_dirty();
    ASSERT_TRUE(timer._dirty.load());

    timer.run();  // successful sync
    ASSERT_FALSE(timer._dirty.load());
    int64_t first_sync = timer.last_sync_time_ms();
    ASSERT_GT(first_sync, 0);

    // Second cycle
    timer.mark_dirty();
    ASSERT_TRUE(timer._dirty.load());
    usleep(2000);  // small gap for time difference
    timer.run();
    ASSERT_FALSE(timer._dirty.load());
    ASSERT_GE(timer.last_sync_time_ms(), first_sync);

    timer.destroy();
    timer.wait_for_destroy();
}

TEST_F(MetaPeriodicSyncTimerTest, adjust_timeout_ms_with_positive_flag) {
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(100));

    // When flag is positive, adjust_timeout_ms returns the flag value
    FLAGS_raft_meta_periodic_sync_interval_ms = 500;
    ASSERT_EQ(500, timer.adjust_timeout_ms(100));

    FLAGS_raft_meta_periodic_sync_interval_ms = 200;
    ASSERT_EQ(200, timer.adjust_timeout_ms(100));

    timer.destroy();
    timer.wait_for_destroy();
}

TEST_F(MetaPeriodicSyncTimerTest, adjust_timeout_ms_zero_falls_back) {
    braft::MetaPeriodicSyncTimer timer(_db, _db_path);
    ASSERT_EQ(0, timer.init(100));

    // When flag is 0, adjust_timeout_ms falls back to the passed-in value
    FLAGS_raft_meta_periodic_sync_interval_ms = 0;
    ASSERT_EQ(100, timer.adjust_timeout_ms(100));
    ASSERT_EQ(250, timer.adjust_timeout_ms(250));

    // Restore
    FLAGS_raft_meta_periodic_sync_interval_ms = 1000;

    timer.destroy();
    timer.wait_for_destroy();
}
