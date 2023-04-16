/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, DISABLED_SampleTest) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // This operation should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(LRUKReplacerTest, EvictTest) {
  LRUKReplacer lru_replacer(7, 3);

  int value;

  // case: [4, 3, 1, 2]
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);

  // evict 3, and remaing [4, 1, 2]
  lru_replacer.Evict(&value);
  ASSERT_THROW(lru_replacer.SetEvictable(3, true), std::runtime_error);

  // record access for frame 3 agin, now [4, 1, 2, 3]
  lru_replacer.RecordAccess(3);

  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);

  lru_replacer.Evict(&value);
  ASSERT_EQ(1, value);

  LRUKReplacer replacer(1000, 3);

  for (int i = 0; i < 1000; i++) {
    replacer.RecordAccess(i);
    replacer.RecordAccess(i);
    replacer.RecordAccess(i);
    replacer.SetEvictable(i, true);
  }

  replacer.Evict(&value);
  ASSERT_EQ(0, value);
}

TEST(LRUKReplacerTest, DISABLED_ConcurrencyTest) {
  const int k = 2;
  const int num_frames = 10;
  LRUKReplacer replacer(num_frames, k);

  // Insert frames into the replacer
  for (int i = 0; i < num_frames; i++) {
    replacer.RecordAccess(i);
    replacer.RecordAccess(i);
    replacer.SetEvictable(i, true);
  }

  ASSERT_EQ(num_frames, replacer.Size());

  // create threads to run evict method
  const int num_threads = 10;
  std::vector<std::thread> threads;
  std::vector<int> evicted_frames(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&replacer, &evicted_frames, i] { replacer.Evict(&evicted_frames[i]); });
  }

  // Wait for all threads to finish
  for (auto &thread : threads) {
    thread.join();
  }

  // verify the results
  //  Check for duplicate evicted frames
  std::sort(evicted_frames.begin(), evicted_frames.end());
  bool has_duplicates = false;
  for (size_t i = 1; i < evicted_frames.size(); i++) {
    if (evicted_frames[i] == evicted_frames[i - 1]) {
      has_duplicates = true;
    }
  }
  EXPECT_FALSE(has_duplicates);
}
}  // namespace bustub
