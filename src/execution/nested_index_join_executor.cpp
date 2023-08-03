//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "catalog/catalog.h"
#include "storage/index/index.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {

  Tuple tuple;
  RID rid;
  IndexInfo *info_inner = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), plan_->GetIndexOid());
  Index *inner_index = info_inner->index_.get();
  while(child_executor_->Next(&tuple, &rid)){
    // info_inner->
    tuple.KeyFromTuple(child_executor_->GetOutputSchema(), plan_->OutputSchema(), const std::vector<uint32_t> &key_attrs)
  }

 }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while(tracked_ < result_.size()){
    *tuple = result_[tracked_];
    *rid = tuple->GetRid();
    tracked_++;
    return true;
  }

  return false;
}

}  // namespace bustub
