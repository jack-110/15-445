//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(i, page_id_temp);
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }

  // the buffer pool is filled up now, buffer: [10, 11, 12, 13, 4, 5, 6, 7, 8, 9],
  // all pages are pinned except page 4.
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(i + buffer_pool_size, page_id_temp);
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  // page 4 will be evicted and be replaced with page 0
  // buffer: [10, 11, 12, 13, 0, 5, 6, 7, 8, 9]
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, page0->GetPageId());
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NewPage test cases
TEST(BufferPoolManagerTest, NewPageTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;

  // case one: the buffer pool is empty
  bpm->NewPage(&page_id_temp);
  EXPECT_EQ(0, page_id_temp);

  // case two: buffer pool is filled up, so all the pages are in use
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    bpm->NewPage(&page_id_temp);
    EXPECT_EQ(i, page_id_temp);
  }

  EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));

  // case three: some buffer pool pages are unpinned and not dirty
  //  unpinned [0], pinned [1, 2, 3, 4, 5, 6, 7, 8, 9]
  bpm->UnpinPage(0, false);

  // buffer pool is filled up, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  auto *page10 = bpm->NewPage(&page_id_temp);
  EXPECT_EQ(10, page_id_temp);

  // cast four: page 10 is evictable and dirty
  snprintf(page10->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page10->GetData(), "Hello"));
  EXPECT_TRUE(bpm->UnpinPage(10, true));

  bpm->NewPage(&page_id_temp);
  EXPECT_EQ(11, page_id_temp);

  // fetch page failed, buffer pool is filled up.
  EXPECT_EQ(nullptr, bpm->FetchPage(10));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, UnpinPageTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;

  // create full buffer pool [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  for (size_t i = 0; i < buffer_pool_size; ++i) {
    bpm->NewPage(&page_id_temp);
    EXPECT_EQ(i, page_id_temp);
  }

  // page id not in the buffer pool
  EXPECT_FALSE(bpm->UnpinPage(10, false));

  // the pin count reach 0
  EXPECT_TRUE(bpm->UnpinPage(0, false));
  EXPECT_FALSE(bpm->UnpinPage(0, false));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, FetchPageTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;

  // create full buffer pool [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  for (size_t i = 0; i < buffer_pool_size; ++i) {
    bpm->NewPage(&page_id_temp);
    EXPECT_EQ(i, page_id_temp);
  }

  bpm->UnpinPage(0, false);

  // disk [0], buffer pool [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  bpm->NewPage(&page_id_temp);

  // case one: buffer pool is filled up, and all pages are pinned
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // case two: buffer pool has evictabel frames, replace old page
  bpm->UnpinPage(1, false);
  auto *page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, page0->GetPageId());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, DeletePageTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  // create full buffer pool [0, 1, 2, 3, 4]
  page_id_t page_id_temp;
  for (size_t i = 0; i < buffer_pool_size; ++i) {
    bpm->NewPage(&page_id_temp);
    EXPECT_EQ(i, page_id_temp);
  }

  // case: page id not in the buffer pool
  EXPECT_TRUE(bpm->DeletePage(5));

  // case: page0 is pinned
  EXPECT_FALSE(bpm->DeletePage(0));

  // case: page0 is unpinned
  bpm->UnpinPage(0, false);
  EXPECT_TRUE(bpm->DeletePage(0));

  // case: when the page0 is deleted
  auto *page0 = bpm->FetchPage(0);
  EXPECT_FALSE(page0->IsDirty());
  EXPECT_EQ(1, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
}  // namespace bustub
