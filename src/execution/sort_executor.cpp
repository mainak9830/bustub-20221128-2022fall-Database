#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() { 
    child_executor_->Init();
    track_ = 0;

    Tuple tuple;
    RID rid;
    while(child_executor_->Next(&tuple, &rid)){
        result_.emplace_back(tuple);
    }

    //comparator custom

    sort(result_.begin(), result_.end(), [&](Tuple const& tuple1, Tuple const& tuple2) {
        
        return true;
    });
 }

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 

    if(track_ < result_.size()){
        *tuple = result_[track_];
        *rid = tuple->GetRid();
        track_++;
        return true;
    }
    return false;

 }

}  // namespace bustub
