//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/delete_executor.h"
#include "storage/table/table_heap.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {  
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    Tuple deleted_tuple;
    RID deleted_rid;
    Transaction *transaction = GetExecutorContext()->GetTransaction();
    LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
    if(completed_){
        return false;
    }

    if (lock_mgr != nullptr) {
        if (transaction->IsTableSharedLocked(plan_->TableOid())) {
        lock_mgr->LockTable(transaction, LockManager::LockMode::EXCLUSIVE, plan_->TableOid());
        } 
    }

    int count = 0;
    while(child_executor_->Next(&deleted_tuple, &deleted_rid)){   

        TableHeap *table_heap = table_info_->table_.get();
        if(!table_heap->MarkDelete(deleted_rid, exec_ctx_->GetTransaction())){
            continue;
        }
        count++;
        for(const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)){
            
            index->index_->DeleteEntry(deleted_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), deleted_rid, exec_ctx_->GetTransaction());
        }
        transaction->GetIndexWriteSet()->emplace_back(IndexWriteRecord(
          deleted_rid, table_info_->oid_, WType::DELETE, deleted_tuple, index->index_oid_, exec_ctx_->GetCatalog()));
        }
    }


    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->UnlockRow(transaction, plan_->TableOid(),deleted_rid);
    }

    Value outval(TypeId::INTEGER, count);
    Tuple output(std::vector<Value>{outval}, &plan_->OutputSchema());
    *tuple = output;

    if(!completed_){
        completed_ = !completed_;
    }
    return completed_;
    
    }
}  // namespace bustub
