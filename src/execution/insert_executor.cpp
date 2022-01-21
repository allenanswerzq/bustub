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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto txn = GetExecutorContext()->GetTransaction();
  if (plan_->IsRawInsert() && !finish) {
    auto values = plan_->RawValues();
    for (auto & v : values) {
      table_info->table_->InsertTuple(Tuple(v, &table_info->schema_), rid, txn);
    }
    finish = true;
    return true;
  }
  else if (plan_->GetType() == PlanType::SeqScan) {
  }
  return false;
}

}  // namespace bustub
