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
#include <vector>
#include "catalog/catalog.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"
#include "type/value.h"

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
  TableHeap* inner_table = exec_ctx_->GetCatalog()->GetTable(plan_->GetIndexOid())->table_.get();
  while(child_executor_->Next(&tuple, &rid)){
    // info_inner->
    // plan_->
    Tuple index_key = tuple.KeyFromTuple(child_executor_->GetOutputSchema(), plan_->OutputSchema(), plan_->OutputSchema().GetUnlinedColumns());
    std::vector<RID> *result={};
    inner_index->ScanKey(index_key, result, exec_ctx_->GetTransaction());

    Tuple inner_tuple;
    for(auto& inner_rid : *result){
      inner_table->GetTuple(inner_rid, &inner_tuple, exec_ctx_->GetTransaction());
     
      auto value = plan_->KeyPredicate()->EvaluateJoin(&tuple, child_executor_->GetOutputSchema(), &inner_tuple, plan_->OutputSchema());
      result_.emplace_back(std::vector<Value>{value}, &GetOutputSchema());
    }
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