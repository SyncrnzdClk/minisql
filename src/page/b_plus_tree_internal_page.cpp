#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  SetKeySize(key_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  // SetKeyAt(0, );
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int start = 1, mid, end = GetSize()-1;
  while (start <= end) {
    mid = (start + end) / 2;
    if (KM.CompareKeys(key, KeyAt(mid)) == -1) {
      end = mid - 1;
    }
    else if (KM.CompareKeys(key, KeyAt(mid)) == 1) {
      start = mid + 1;
    }
    else {
      return ValueAt(mid);
    }
  }
  return ValueAt(end);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // this page is the new root, the allocation and initialization of this page is finished in the InsertIntoParent()
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
  SetKeyAt(1, new_key);
  // SetKeyAt(0, new_key);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  for (int i = 0; i < GetSize(); i++) { // iterate through the page to find the old value (old page id)
    if (ValueAt(i) == old_value) {
      // insert the new key value pair into the data array
      for (int j = GetSize(); j > i+1; j--) {
        SetKeyAt(j, KeyAt(j-1));
        SetValueAt(j, ValueAt(j-1));
      }
      SetKeyAt(i+1, new_key);
      SetValueAt(i+1, new_value);
      SetSize(GetSize()+1);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  // note that i'm not sure whether i have to  manage this page in this process like the pages that are copied to the recipient page(change parent id)
  // i guess i'll do this in some other fucntions
  int mid_index = GetSize() / 2;
  recipient->CopyNFrom(pairs_off + mid_index * pair_size, GetSize() - mid_index, buffer_pool_manager);
  SetSize(mid_index);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  // get the start index of the page
  int start_index = GetSize();

  // copy the pairs into the page
  PairCopy(pairs_off + start_index * pair_size, src, size);

  // reset the parent page_id of the pages that have been copied to the current internal page
  // notice that here we cast the page to type bplustreepage but not bplustreeinternalpage because it could be a leaf page
  for (int i = 0; i < size; i++) {
    auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(ValueAt(start_index))->GetData());
    page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }

  SetSize(start_index + size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  for (int i = index; i < GetSize()-1; i++) {
    SetValueAt(i, ValueAt(i+1));
    SetKeyAt(i, KeyAt(i+1));
  }
  SetSize(GetSize()-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t returnPageId = ValueAt(0);
  SetSize(0);
  return returnPageId;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key); // set the invalid key as the middle key
  recipient->CopyNFrom(pairs_off, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  // move the following contents forwawrd to make sure they are continuously stored
  for (int i = 0; i < GetSize()-1; i++) {
    if (i != 0) SetKeyAt(i, KeyAt(i+1));
    SetValueAt(i, ValueAt(i+1));
  }
  SetSize(GetSize()-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int end_index = GetSize(); // get the index to put the new pair in
  // insert the new pair into the page
  SetValueAt(end_index, value);
  SetKeyAt(end_index, key);

  // update the inserted page's parent id
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(GetPageId(), true);

  SetSize(end_index + 1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int end_index = GetSize()-1;
  recipient->CopyFirstFrom(ValueAt(end_index), buffer_pool_manager);
  // updating the recipient's array, move every key backward
  for (int i = recipient->GetSize(); i > 0; i--) {
    if (i != 1) recipient->SetKeyAt(i, KeyAt(i-1));
  }
  // insert the middle key into the recipient's array
  recipient->SetKeyAt(1, middle_key);
  
  SetSize(end_index);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // move the values backward to leave space for the first inserted value
  for (int i = GetSize(); i > 0; i--) {
    SetValueAt(i, ValueAt(i-1));
  }
  // insert the moved page
  SetValueAt(0, value);

  // reset the page's parent page id
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  
  SetSize(GetSize()+1);
}