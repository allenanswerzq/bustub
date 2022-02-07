//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_executor_.get(); }

void AggregationExecutor::Init() {
  BUSTUB_ASSERT(child_executor_, "child executor must exist");
  child_executor_->Init();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  RID cur_rid;
  if (!first) {
    first = true;
    // Executor breaker
    while (child_executor_->Next(&cur_tuple, &cur_rid)) {
      aht_.InsertCombine(MakeKey(&cur_tuple), MakeVal(&cur_tuple));
    }
    aht_iterator_ = aht_.Begin();
  }
  std::vector<Value> values;
  for (; aht_iterator_ != aht_.End(); ) {
    // For each group specified using group by clause
    AggregateKey key = aht_iterator_.Key();
    AggregateValue val = aht_iterator_.Val();
    const AbstractExpression * having = plan_->GetHaving();
    if (!having || having->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
      std::vector<Value> value;
      for (const Column& column : plan_->OutputSchema()->GetColumns()) {
        auto expr = column.GetExpr();
        auto v = expr->EvaluateAggregate(key.group_bys_, val.aggregates_);
        value.push_back(v);
      }
      cur_tuple = Tuple(value, plan_->OutputSchema());
      if (tuple) {
        *tuple = cur_tuple;
      }
      if (rid) {
        *rid = RID();
      }
      ++aht_iterator_;
      // Returns if we get a tuple for any group
      return true;
    }
    else {
      // This tuple is filtered, try next one
      ++aht_iterator_;
    }
  }
  return false;
}

}  // namespace bustub
