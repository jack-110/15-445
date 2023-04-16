//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, ReadPageGuardTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = ReadPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  // case:move constructor
  auto guarded_page_move = std::move(guarded_page);

  EXPECT_EQ(page0->GetData(), guarded_page_move.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page_move.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page_move.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // case: move assignment operator
  auto page1 = bpm->NewPage(&page_id_temp);
  auto page2 = bpm->NewPage(&page_id_temp);

  auto guarded_page1 = ReadPageGuard(bpm.get(), page1);
  auto guarded_page2 = ReadPageGuard(bpm.get(), page2);

  guarded_page1 = std::move(guarded_page2);

  EXPECT_EQ(page2->GetData(), guarded_page1.GetData());
  EXPECT_EQ(page2->GetPageId(), guarded_page1.PageId());
  EXPECT_EQ(1, page2->GetPinCount());

  guarded_page1.Drop();

  EXPECT_EQ(0, page2->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
