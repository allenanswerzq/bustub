//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  BUSTUB_ASSERT(left_executor_, "left executor must exist");
  BUSTUB_ASSERT(right_executor_, "left executor must exist");
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple, right_tuple;
  RID left_rid, right_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      const Schema* left_schema = plan_->GetLeftPlan()->OutputSchema();
      const Schema* right_schema = plan_->GetRightPlan()->OutputSchema();
      const Schema* out_final =  plan_->OutputSchema();
      Value result = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (result.GetAs<bool>()) {
        std::vector<Value> values;
        for (const Column& column : out_final->GetColumns()) {
          auto column_name = column.GetName();
          auto expr = column.GetExpr();
          if (left_schema->hasColumn(column_name)) {
            // TODO(zhangqiang): Any other ways to do this without define this inteface
            values.push_back(expr->Evaluate(&left_tuple, left_schema));
          } else {
            values.push_back(expr->Evaluate(&right_tuple, right_schema));
          }
        }
        Tuple cur_tuple(values, out_final);
        if (tuple) {
          *tuple = cur_tuple;
        }
        if (rid) {
          *rid = RID();
        }
        return true;
      }
    }
    right_executor_->Init();
  }
  return false;
}

}  // namespace bustub
