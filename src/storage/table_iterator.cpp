#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap(table_heap) {
  row = Row(rid);
  // if(!(rid == INVALID_ROWID)) table_heap->GetTuple(&row, txn);
}
// personal added function
TableIterator::TableIterator(TableHeap *table_heap, Row row_, Txn *txn) : table_heap(table_heap) {
  row = row_;
}
TableIterator::TableIterator(const TableIterator &other) {
  table_heap = new TableHeap(other.table_heap->buffer_pool_manager_, other.table_heap->first_page_id_, other.table_heap->schema_, other.table_heap->log_manager_, other.table_heap->lock_manager_);
  row = other.row;
  // txn = new Txn(*other.txn) // not implemented yet
}

TableIterator::~TableIterator() {
  // delete[] table_heap;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (this->table_heap == itr.table_heap) && (this->row.GetRowId() == itr.row.GetRowId());
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return row;
  // else not implemented yet
}

Row *TableIterator::operator->() {
  return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  // ASSERT(false, "Not implemented yet.");
  table_heap = itr.table_heap;
  row = itr.row;
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(row.GetRowId().GetPageId()));
  RowId nextRowId;
  if (page->GetNextTupleRid(row.GetRowId(), &nextRowId)) { // if the current page has rows behind the current row, update the rowid
    row.destroy();
    row.SetRowId(nextRowId);
    table_heap->GetTuple(&row, nullptr);
  }
  else { // else, check if there are pages behind the current pages
    while (page->GetNextPageId() != INVALID_PAGE_ID) {
      auto nextPage = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      table_heap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      if (nextPage->GetFirstTupleRid(&nextRowId)) { // scan the rest of the pages until the next row is found
        // table_heap->buffer_pool_manager_->UnpinPage(nextPage->GetPageId(), false);
        // nextPage = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(nextPage->GetNextPageId()));
        // if (nextPage == nullptr) { // if the iterator is at the end of the table_heap, just return a invalid iterator
        //   *this = TableIterator(nullptr, RowId(), nullptr);
        //   return *this;
        row.destroy();
        row.SetRowId(nextRowId);
        table_heap->GetTuple(&row, nullptr);
        table_heap->buffer_pool_manager_->UnpinPage(nextPage->GetPageId(), false);
        return *this;
      }
      page = nextPage;
    }
    table_heap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    *this = table_heap->End();
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this);
  ++(*this);
  return TableIterator(temp);
}
