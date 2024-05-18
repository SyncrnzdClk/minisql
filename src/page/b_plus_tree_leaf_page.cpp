#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
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
      return mid;
    }
  }
  return end + 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int start = 1, mid, end = GetSize() - 1;
  // find the place to insert the pair
  while (start <= end) {
    mid = (start + end) / 2;
    if (KM.CompareKeys(key, KeyAt(mid)) == -1) {
      end = mid - 1;
    }
    else if (KM.CompareKeys(key, KeyAt(mid)) == 1) {
      start = mid + 1;
    }
    else {
      SetValueAt(mid, value);
      return GetSize();
    }
  }
  // insert the new pair
  for (int i = GetSize(); i > end + 1; i--) {
    SetKeyAt(i, KeyAt(i-1));
    SetValueAt(i, ValueAt(i-1));
  }
  SetKeyAt(end + 1, key);
  SetValueAt(end + 1, value);
  SetSize(GetSize() + 1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int start = 1, mid, end = GetSize() - 1;
  // find the place of the key
  while (start <= end) {
    mid = (start + end) / 2;
    if (KM.CompareKeys(key, KeyAt(mid)) == -1) {
      end = mid - 1;
    }
    else if (KM.CompareKeys(key, KeyAt(mid)) == 1) {
      start = mid + 1;
    }
    else {
      value = ValueAt(mid);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int start = 1, mid, end = GetSize() - 1;
  // find the place of the key
  while (start <= end) {
    mid = (start + end) / 2;
    if (KM.CompareKeys(key, KeyAt(mid)) == -1) {
      end = mid - 1;
    }
    else if (KM.CompareKeys(key, KeyAt(mid)) == 1) {
      start = mid + 1;
    }
    else {
      // perform deletion
      for (int i = mid; i < GetSize()-1; i++) {
        SetValueAt(i, ValueAt(i+1));
        SetKeyAt(i, KeyAt(i+1));
      }
      // return page size after deletion
      SetSize(GetSize()-1);
      return GetSize();
    }
  }
  return -1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  SetKeyAt(size, key);
  SetValueAt(size, value);
  SetSize(size + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  for (int i = size; i > 0; i--) {
    SetKeyAt(i, KeyAt(i-1));
    SetValueAt(i, ValueAt(i-1));
  }
  SetSize(size + 1);
  SetKeyAt(0, key);
  SetValueAt(0, value);
}