//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    catalog_ = exec_ctx_->GetCatalog();
    table_info_ = catalog_->GetTable(plan_->TableOid());
    table_heap_ = table_info_->table_.get();
    child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    
    // std::vector<Tuple> child_tuples;
    // child_tuples.push_back(ctuple);
    if(completed_){
        return false;
    }
    // std::cout << "size of clolumns in input schema" << table_info_->schema_.GetColumns().size() << std::endl; 
    int count = 0;
    while(child_executor_->Next(tuple, rid)){
        // return false; 
        // std::cout<<*rid<< " " << tuple->ToString(&plan_->OutputSchema()) << std::endl;
        if(!table_heap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())){
            throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:no enough space for this tuple.");
        }
        // std::cout<< catalog_->GetTableIndexes(table_info_->name_).size() << " ";
        for(const auto &index : catalog_->GetTableIndexes(table_info_->name_)){
            
            index->index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid, exec_ctx_->GetTransaction());
        }
        
        count++;
    }
    
    Value outval(TypeId::INTEGER, count);
    Tuple output(std::vector<Value>{outval}, &plan_->OutputSchema());
    *tuple = output;
    
    // plan_->OutputSchema()
    if(!completed_){
        completed_ = !completed_;
    }
    return completed_;
    
}

}  // namespace bustub
