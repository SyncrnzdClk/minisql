#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  // ASSERT(false, "Not implemented yet.");
  GenericKey* key = page->KeyAt(item_index);
  RowId row_id = page->ValueAt(item_index);
  return make_pair(key, row_id);
}

IndexIterator &IndexIterator::operator++() {
  // ASSERT(false, "Not implemented yet.");
  if (item_index != (page->GetSize() - 1)) { // the next item is still in this page, update the item_index
    item_index++;
  }
  else { // unpin the current page, get the next page, reset the item index
    page_id_t next_page_id = page->GetNextPageId();
    buffer_pool_manager->UnpinPage(page->GetPageId(), false);
    if (next_page_id != INVALID_PAGE_ID) {
      page = reinterpret_cast<BPlusTreeLeafPage*>(buffer_pool_manager->FetchPage(next_page_id)->GetData());
      item_index = 0;
      current_page_id = next_page_id;
    }
    else { // at the end of the b plus tree
      page = nullptr;
      current_page_id = INVALID_PAGE_ID;
      item_index = 0;
    }
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}