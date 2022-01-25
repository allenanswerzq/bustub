//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto txn = GetExecutorContext()->GetTransaction();

  Tuple cur_tuple;
  RID cur_rid;
  while (child_executor_->Next(&cur_tuple, &cur_rid)) {
    table_info->table_->MarkDelete(cur_rid, txn);

    // Also delete this tuple from indexes this table has.
    auto indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
    for (size_t i = 0; i < indexes.size(); i++) {
      IndexInfo* index_info = indexes[i];
      auto index = index_info->index_.get();
      index->DeleteEntry(cur_tuple, cur_rid, txn);
    }

    if (tuple) {
      *tuple = cur_tuple;
    }
    if (rid) {
      *rid = cur_rid;
    }
    return true;
  }
  return false;
}

}  // namespace bustub
