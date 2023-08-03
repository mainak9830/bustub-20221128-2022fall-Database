//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <cstddef>
#include "catalog/catalog.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), 
    iter_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())->GetBeginIterator()),
    catalog_(exec_ctx_->GetCatalog()){}

void IndexScanExecutor::Init() { 
    // catalog_->
    table_heap_ = catalog_->GetTable(plan_->GetIndexOid())->table_.get();
    // exec_ctx_->GetCatalog()->GetTable(plan_->GetTa())
    // plan_->GetIndexOid()
    // exec_ctx_->GetCatalog()->GetTable()
    // table_heap_ = ->GetTable(plan_->GetIndexOid())
    
    
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(iter_.IsEnd()) {
        // std::cout << "Not FOund" << std::endl;
        return false;
    }
    // const Schema *output_schema = &GetOutputSchema();
    // table_heap_
    // (*iter_)
    *rid = (*iter_).second;
    
    if(!table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction())){
        std::cout << " hey there" << *rid << std::endl;
        return false;
    }
    
    iter_ = ++iter_;
    return true; 
    }
}  // namespace bustub
