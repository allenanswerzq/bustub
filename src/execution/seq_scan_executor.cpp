//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
  : AbstractExecutor(exec_ctx), plan_(plan), it_(TableIterator(nullptr, RID(INVALID_PAGE_ID, 0), nullptr)) {}

void SeqScanExecutor::Init() {
  auto table_id = plan_->GetTableOid();
  auto txn = GetExecutorContext()->GetTransaction();
  it_ = GetExecutorContext()->GetCatalog()->GetTable(table_id)->table_->Begin(txn);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto table = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  auto predicate = plan_->GetPredicate();
  for (; it_ != table->End(); it_++) {
    Tuple tmp = *it_;
    if (!predicate || predicate->Evaluate(&tmp, GetOutputSchema()).GetAs<bool>()) {
      if (tuple) {
        *tuple = tmp;
      }
      if (rid) {
        *rid = it_->GetRid();
      }
      it_++;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
