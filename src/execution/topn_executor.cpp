#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {}

void TopNExecutor::Init() { 
    child_executor_->Init();
 }

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    return child_executor_->Next(tuple, rid);
}

}  // namespace bustub
