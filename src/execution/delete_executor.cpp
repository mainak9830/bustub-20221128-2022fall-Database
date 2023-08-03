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

    if(completed_){
        return false;
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
