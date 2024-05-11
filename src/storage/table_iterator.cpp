#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap(table_heap), rid(rid) {}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap = new TableHeap(other.table_heap->buffer_pool_manager_, other.table_heap->first_page_id_, other.table_heap->schema_, other.table_heap->log_manager_, other.table_heap->lock_manager_);
  rid = other.rid;
  // txn = new Txn(*other.txn) // not implemented yet
}

TableIterator::~TableIterator() {
  delete[] table_heap;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (this->table_heap == itr.table_heap) && (this->rid == itr.rid);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return (this->table_heap != itr.table_heap) || !(this->rid == itr.rid); // RowId does not have operator !=
}

const Row &TableIterator::operator*() {
  Row row(rid);
  if (table_heap->GetTuple(&row, nullptr)) return row;
  // else not implemented yet
}

Row *TableIterator::operator->() {
  Row row(rid);
  if (table_heap->GetTuple(&row, nullptr)) return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  // ASSERT(false, "Not implemented yet.");
  *table_heap = *(itr.table_heap);
  rid = itr.rid;
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  RowId nextRowId;
  if (page->GetNextTupleRid(rid, &nextRowId)) { // if the current page has rows behind the current row, update the rowid
    rid = nextRowId;
  }
  else { // else, check if there are pages behind the current pages
    if (page->GetNextPageId() != INVALID_PAGE_ID) {
      auto nextPage = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      while (!(nextPage->GetFirstTupleRid(&nextRowId))) { // scan the rest of the pages until the next row is found
        table_heap->buffer_pool_manager_->UnpinPage(nextPage->GetPageId(), false);
        nextPage = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(nextPage->GetNextPageId()));
        if (nextPage == nullptr) { // if the iterator is at the end of the table_heap, just return a invalid iterator
          *this = TableIterator(nullptr, RowId(), nullptr);
          return *this;
        }
      }
      table_heap->buffer_pool_manager_->UnpinPage(nextPage->GetPageId(), false);
      rid = nextRowId;
    }
    table_heap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this);
  ++(*this);
  return TableIterator(temp);
}
