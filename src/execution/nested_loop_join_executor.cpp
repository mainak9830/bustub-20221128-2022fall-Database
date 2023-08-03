//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;

  left__executor_->Init();
  

  while(left__executor_->Next(&left_tuple, &left_rid)){
    right__executor_->Init();
    while(right__executor_->Next(&right_tuple, &right_rid)){
       
      auto value = plan_->predicate_->EvaluateJoin(&left_tuple, left__executor_->GetOutputSchema(), &right_tuple,
                                                       right__executor_->GetOutputSchema());
      result_.emplace_back(std::vector<Value>{value}, &GetOutputSchema());

    }
  }

}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while(tracked_ < result_.size()){
    *tuple = result_[tracked_];
    *rid = tuple->GetRid();
    tracked_++;
    return true;
  }

  return false;
}
}  // namespace  bustub

