//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  position_ = 0;
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto txn = GetExecutorContext()->GetTransaction();
  if (plan_->IsRawInsert()) {
    auto values = plan_->RawValues();
    if (position_ < values.size()) {
      auto v = values[position_++];

      RID cur_rid;
      Tuple cur_tuple(v, &table_info->schema_);
      table_info->table_->InsertTuple(cur_tuple, &cur_rid, txn);

      // Also insert this tuple into indexes this table has.
      for (IndexInfo* index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_)) {
        auto index = index_info->index_.get();
        index->InsertEntry(cur_tuple, cur_rid, txn);
      }

      return true;
    }
  }
  else if (child_executor_) {
    Tuple cur_tuple;
    RID cur_rid;
    while (child_executor_->Next(&cur_tuple, &cur_rid)) {
      bool result = table_info->table_->InsertTuple(cur_tuple, &cur_rid, txn);
      BUSTUB_ASSERT(result, "Insert into table failed.");

      // Also insert this tuple into indexes this table has.
      for (IndexInfo* index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_)) {
        auto index = index_info->index_.get();
        index->InsertEntry(cur_tuple, cur_rid, txn);
      }

      if (tuple) {
        *tuple = cur_tuple;
      }
      if (rid) {
        *rid = cur_rid;
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
