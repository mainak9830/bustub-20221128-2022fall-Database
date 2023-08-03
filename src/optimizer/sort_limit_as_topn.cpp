#include <cstddef>
#include <memory>
#include <vector>
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule

  std::vector<AbstractPlanNodeRef> children;
  for(const auto& child : plan->GetChildren()){
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if(optimized_plan->GetType() == PlanType::TopN){
    const auto &top_plan_node = dynamic_cast<const TopNPlanNode &>(*optimized_plan);

    auto& child_node = children[0];
    size_t top_n  = top_plan_node.GetN();
    SortPlanNode sort_node(std::make_shared<Schema>(plan->OutputSchema()), child_node, top_plan_node.GetOrderBy());
    // AbstractPlanNodeRef limitnoderef = std::shared_ptr<LimitPlanNode>(limit_node);-
    AbstractPlanNodeRef plan1 = sort_node.CloneWithChildren(child_node->children_);
    LimitPlanNode limit_node(std::make_shared<Schema>(plan1->OutputSchema()), plan1, top_n);
    auto plan2 = limit_node.CloneWithChildren({plan1});
    return plan2;
  }
  return optimized_plan;
}

}  // namespace bustub
