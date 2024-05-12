#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
  page_id_t curPageId = first_page_id_;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(curPageId));
  if (page == nullptr) return false; // the buffer pool is full currently

  while (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_) != true) {
    buffer_pool_manager_->UnpinPage(curPageId, false);
    curPageId = page->GetNextPageId();
    if (curPageId != INVALID_PAGE_ID) { // if there are still pages left, just update the page to the next page
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(curPageId));
    }
    else { // if there are no more pages left, create a new page
      TablePage* newTablePage = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(curPageId));
      if (curPageId = INVALID_PAGE_ID) return false; // if there are no more free pages, return false
      newTablePage->Init(curPageId, page->GetPageId(), log_manager_, txn);

      // insert the tuple into the new page
      newTablePage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);

      // set the prev page's next pageId as the new page's pageId
      page->SetNextPageId(curPageId);
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(curPageId, true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Txn *txn) { 

  auto page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(rid.GetPageId())); // fetch the page that contains the row
  if (page == nullptr) return false; // if the buffer pool manager is full, return false
  Row targetRow = Row(row);
  targetRow.SetRowId(rid);

  int flag = page->UpdateTuple(row, &targetRow, schema_, txn, lock_manager_, log_manager_);

  // if the updated row can be fit into the page, return true after update
  if (flag == 1) {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true); 
    return true;
  }
  else if (flag == 0 || flag == -1) {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
  // if the updated row is too large to fit into the page, delete the former row in the page, and insert the updated row into a new page
  else {
    page->ApplyDelete(rid, txn, log_manager_); // delete the former row
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    if (InsertTuple(targetRow, txn)) return true; // if the insertion is successful
    else return false;  // the insertion is unsuccessful
  }  

}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return;
  }
  page->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) return false;
  if (page->GetTuple(row, schema_, txn, lock_manager_)) {
    buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
    return true;
  }
  else return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  if (first_page_id_ == INVALID_PAGE_ID) return TableIterator(nullptr, RowId(INVALID_ROWID), nullptr); // if there wasn't any page, just return a ending iterator

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId())); // get the first page of the tableheap
  RowId iteratorRowId;
  while (!page->GetFirstTupleRid(&iteratorRowId)) { // search the table heap until we find the first row or reach the end of the table heap
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    if (page->GetNextPageId() != INVALID_PAGE_ID) page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId())); 
    else return TableIterator(nullptr, RowId(INVALID_ROWID), nullptr); // reach the end of the table heap
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return TableIterator(this, iteratorRowId, txn); 
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(INVALID_ROWID), nullptr); }
