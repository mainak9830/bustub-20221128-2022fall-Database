//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <map>
#include <set>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
  
  std::unique_lock<std::mutex> ul(table_lock_map_latch_);

  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){

    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      return false;
    }

    if(txn->GetState() == TransactionState::ABORTED){
      return false;
    }
   
  }

  if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      return false;
    }

    if(txn->GetState() == TransactionState::ABORTED){
      return false;
    }
  }

  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    if(txn->GetState() == TransactionState::SHRINKING){
      txn->SetState(TransactionState::ABORTED);
      return false;
    }

    if(txn->GetState() == TransactionState::ABORTED){
      return false;
    }
  }
  std::map<LockManager::LockMode, std::set<LockManager::LockMode>> possible_states_all;

  possible_states_all[LockMode::INTENTION_SHARED] = {
  LockMode::SHARED,
  LockMode::INTENTION_SHARED,
  LockMode::INTENTION_EXCLUSIVE,
  LockMode::SHARED_INTENTION_EXCLUSIVE
  };

  possible_states_all[LockMode::EXCLUSIVE] = {
 
  };

  possible_states_all[LockMode::INTENTION_EXCLUSIVE] = {
  LockMode::INTENTION_SHARED,
  LockMode::INTENTION_EXCLUSIVE,
  };

  possible_states_all[LockMode::SHARED] = {
  LockMode::SHARED,
  LockMode::INTENTION_SHARED
  };

  possible_states_all[LockMode::SHARED_INTENTION_EXCLUSIVE] = {
  LockMode::INTENTION_SHARED
  };

  std::map<LockManager::LockMode, std::set<LockManager::LockMode>> upgrade_states_all;
  upgrade_states_all[LockMode::INTENTION_SHARED] = {
  LockMode::SHARED,
  LockMode::EXCLUSIVE,
  LockMode::INTENTION_EXCLUSIVE,
  LockMode::SHARED_INTENTION_EXCLUSIVE
  };

  upgrade_states_all[LockMode::SHARED] = {
    LockMode::EXCLUSIVE,
    LockMode::SHARED_INTENTION_EXCLUSIVE
  };

  upgrade_states_all[LockMode::INTENTION_EXCLUSIVE] = {
    LockMode::EXCLUSIVE,
    LockMode::SHARED_INTENTION_EXCLUSIVE
  };

  upgrade_states_all[LockMode::SHARED_INTENTION_EXCLUSIVE] = {
    LockMode::EXCLUSIVE,
  };

  upgrade_states_all[LockMode::EXCLUSIVE] = {
    LockMode::EXCLUSIVE,
  };
  auto request_queue = table_lock_map_[oid].get()->request_queue_;
  auto upgrade_states = upgrade_states_all[lock_mode]; 
  auto possible_states = possible_states_all[lock_mode];
  // If its already in queue and 
  // not granted then another transcation request with the same resource should not come, 
  // because first one was blocked
  bool already_locked = false;
  for(auto &it : request_queue){
    if(it->txn_id_ == txn->GetTransactionId() && it->granted_){
      //try upgrade lock
      already_locked = true;
      if(upgrade_states.count(lock_mode) == 0){
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }
      
    }
  }

  if(already_locked){
    //try and wait
  }else{
    LockRequest req(txn->GetTransactionId(), lock_mode, oid);
    table_lock_map_[oid].get()->request_queue_.emplace_back(req);
  }

  
  //suppose its all shared and I am asking for exclusive
  while(true){
    bool flag = false;
    for(auto &it : request_queue){

      if(possible_states.count(it->lock_mode_) == 0){

          flag = true;
          break;
      }
      
      
    }

    if(!flag){
      
      break;
    }
    table_lock_map_[oid].get()->cv_.wait(ul);

    
    //check for any upgrade locks first          
    
  }
  
  for(auto &it : request_queue){

    if(it->txn_id_ == txn->GetTransactionId()){

        it->granted_ = true;
        break;
    }
      
      
  }
  txn->GetSharedTableLockSet()->emplace(oid);
    
    return true;
}

auto GetLockMode(Transaction* txn, const table_oid_t &oid) -> LockManager::LockMode{
  if(txn->IsTableExclusiveLocked(oid)){
    return LockManager::LockMode::EXCLUSIVE;
  }
  if(txn->IsTableIntentionExclusiveLocked(oid)){
    return LockManager::LockMode::INTENTION_EXCLUSIVE;
  }
  if(txn->IsTableSharedIntentionExclusiveLocked(oid)){
    return LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if(txn->IsTableSharedLocked(oid)){
    return LockManager::LockMode::SHARED;
  }
  if(txn->IsTableIntentionSharedLocked(oid)){
    return LockManager::LockMode::INTENTION_SHARED;
  }
 
  return LockManager::LockMode::NOT_LOCKED;

}
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  std::unique_lock<std::mutex> ul(table_lock_map_latch_);


  auto request_queue = table_lock_map_[oid].get()->request_queue_;
  auto it = request_queue.begin();
  while(it != request_queue.end()){
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      it = request_queue.erase(it);
      switch (GetLockMode(txn, oid)) {
        case LockMode::EXCLUSIVE: {
          txn->GetExclusiveTableLockSet()->erase(oid);
          if (!request_queue.empty()) {
             table_lock_map_[oid].get()->cv_.notify_all();
          }
          break;
        }
        case LockMode::SHARED: {
          txn->GetExclusiveTableLockSet()->erase(oid);
          table_lock_map_[oid].get()->cv_.notify_all();
          break;
        }

        case LockMode::INTENTION_EXCLUSIVE: {
          txn->GetIntentionExclusiveTableLockSet()->erase(oid);
          table_lock_map_[oid].get()->cv_.notify_all();
          break;
        }

        case LockMode::SHARED_INTENTION_EXCLUSIVE: {
          txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
          table_lock_map_[oid].get()->cv_.notify_all();
          break;
        }

        case LockMode::INTENTION_SHARED: {
          txn->GetIntentionSharedTableLockSet()->erase(oid);
          table_lock_map_[oid].get()->cv_.notify_all();
          break;
        }
        case LockMode::NOT_LOCKED:{
          return false;
        }
        
      }
    }
  }
  return false; 
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  return true; 
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  for(int t : waits_for_[t1]){
    if(t == t2){
      return;
    }
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  for(auto it = waits_for_[t1].begin(); it != waits_for_[t1].end();it++){
    it = waits_for_[t1].erase(it);
  }
  
}
auto LockManager::Dfs(txn_id_t u, txn_id_t *txn_id, std::set<txn_id_t>& visited) -> bool{
  
  for(auto v : waits_for_[u]){
    
    if(visited.count(v) != 0U){
      *txn_id = fmin(u, v);
      for(auto it = waits_for_[u].begin();it != waits_for_[u].end();it++){
        if(*it == v){
          waits_for_[u].erase(it);
          return true;
        }
        
      }
      
      
    }
    
    if(Dfs(v, txn_id, visited)){
      return true;
    }
  }
  return false;
}
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 

  for(auto& it : waits_for_){
    std::set<txn_id_t> visited;
    if(Dfs(it.first, txn_id, visited)){
      return true;
    }
  }
  return false; 
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock

      for(auto& it : table_lock_map_){
        auto request_queue = table_lock_map_[it.first]->request_queue_;
        for(auto& t1_req : request_queue){
          if(!t1_req->granted_ ){
            for(auto& t2_req: request_queue){
              if(t2_req->granted_){
                AddEdge(t1_req->txn_id_, t2_req->txn_id_);
              }
            }
          }
        }
      }

      for(auto& it : row_lock_map_){
        auto request_queue = row_lock_map_[it.first]->request_queue_;
        for(auto& t1_req : request_queue){
          if(!t1_req->granted_ ){
            for(auto& t2_req: request_queue){
              if(t2_req->granted_){
                AddEdge(t1_req->txn_id_, t2_req->txn_id_);
              }
            }
          }
        }
      }

      
      txn_id_t loop;
      while(HasCycle(&loop)){
        TransactionManager::GetTransaction(loop)->SetState(TransactionState::ABORTED);
      }

      waits_for_.clear();
    }
  }
}

}  // namespace bustub
