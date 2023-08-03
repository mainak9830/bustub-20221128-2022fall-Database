//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() { 
    
    table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
    iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
 }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(iter_ == table_heap_->End()) {
        return false;
    }
    // const Schema *output_schema = &GetOutputSchema();

    *tuple = *iter_;
    *rid = iter_->GetRid();
    ++iter_;
    return true; 
}

}  // namespace bustub
