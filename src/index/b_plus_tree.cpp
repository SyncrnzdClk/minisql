#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
        root_page_id_ = INVALID_PAGE_ID;
        // // initialize the root page
        // auto root_index_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->NewPage(root_page_id_));
        // if (!root_index_page->GetRootId(index_id, &root_page_id_)) {
        //   // if the GetRootId doesn't return a valid root page id
        //   root_page_id_ = INVALID_PAGE_ID;
        // }
        // else {
        //   // if successfully get the root id, initialize the root page
        //   auto root_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager->FetchPage(root_page_id_)->GetData());
        //   root_page->Init(root_page_id_, INVALID_PAGE_ID, KM.GetKeySize(), leaf_max_size);
        //   buffer_pool_manager->UnpinPage(root_page_id_, true);
        // }
        // buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  // i guess the upper layer would call this function recursively to destroy every page
  buffer_pool_manager_->UnpinPage(current_page_id, true);
  buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  // // check the whether the root page size equals 0
  // auto root_page =  reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  // buffer_pool_manager_->UnpinPage(root_page_id_, false);
  // return root_page->GetSize() == 0;
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, bool miao, Txn *transaction) {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto root = reinterpret_cast<BPlusTreePage *>(page->GetData());
  bool flag = false;
  if (root->IsLeafPage()) { // if the root is a leaf, just check if the key is inside it
    auto leaf_root = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
    RowId res;
    flag = leaf_root->Lookup(key, res, processor_);
    if (flag) result.push_back(res);
  }
  else { // if the root is not the key, first iteratively find the leaf that might contain the key, and then check if the key is inside of it
    auto internal_root = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    page_id_t next_id = internal_root->Lookup(key, processor_);
    auto next_page = buffer_pool_manager_->FetchPage(next_id);
    while(!reinterpret_cast<BPlusTreePage *>(next_page->GetData())->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(next_id, false); // unpin the current page first (or the next_id is going to be updated)
      next_id = reinterpret_cast<BPlusTreeInternalPage *>(next_page->GetData())->Lookup(key, processor_);
      next_page = buffer_pool_manager_->FetchPage(next_id);
    }
    RowId res;

    // if the leaf page is found
    flag = reinterpret_cast<BPlusTreeLeafPage *>(next_page->GetData())->Lookup(key, res, processor_);
    result.push_back(res);
    buffer_pool_manager_->UnpinPage(next_id, false);
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) { // if the current b plus tree is empty, start a new tree
    StartNewTree(key, value);
    return true;
  }
  else { // else insert the pair into the leaf
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t root_page_id;
  auto root_page = buffer_pool_manager_->NewPage(root_page_id);
  if (root_page == nullptr) { // if there aren't enough pages 
    throw std::runtime_error("out of memory");
  }
  else {
    root_page_id_ = root_page_id;
    // update the information in the index roots page
    auto index_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
    index_page->Insert(index_id_, root_page_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);

    // intialize the bplustree root page
    auto bplus_root_page = reinterpret_cast<BPlusTreeLeafPage *>((root_page->GetData()));
    bplus_root_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);

    // finally insert the key
    bplus_root_page->Insert(key, value, processor_);

    // unpin the root page
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  Page* leaf_to_be_inserted = FindLeafPage(key, root_page_id_);
  auto bplus_leaf_to_be_inserted = reinterpret_cast<BPlusTreeLeafPage *>(leaf_to_be_inserted->GetData());
  RowId temp;
  if (bplus_leaf_to_be_inserted->Lookup(key, temp, processor_)) { // if the key exist in the leaf, return immediately
    return false;
  }
  else { // insert the entry
    int size;
    size = bplus_leaf_to_be_inserted->Insert(key, value, processor_);
    if (size > leaf_max_size_) {
      // if the size after insertion exceeds the max size, perform split
      BPlusTreeLeafPage* split_page = Split(bplus_leaf_to_be_inserted, transaction);
      // update the next page id of the leaf nodes
      split_page->SetNextPageId(bplus_leaf_to_be_inserted->GetNextPageId());
      bplus_leaf_to_be_inserted->SetNextPageId(split_page->GetPageId());
      InsertIntoParent(bplus_leaf_to_be_inserted, split_page->KeyAt(0), split_page);
      buffer_pool_manager_->UnpinPage(leaf_to_be_inserted->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(split_page->GetPageId(), true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(leaf_to_be_inserted->GetPageId(), true);
    return true;

  }
  
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t split_page_id;
  // allocate a new page for the splited page
  auto split_page = buffer_pool_manager_->NewPage(split_page_id);
  if (split_page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  auto bplus_split_page = reinterpret_cast<BPlusTreeInternalPage *>(split_page->GetData());
  bplus_split_page->Init(split_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  node->MoveHalfTo(bplus_split_page, buffer_pool_manager_);
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t split_page_id;
  // alocate a new page for the splited page, and notice here we do not unpin it (we'll do it in the function that call this)
  auto split_page = buffer_pool_manager_->NewPage(split_page_id);
  if (split_page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  auto bplus_split_page = reinterpret_cast<BPlusTreeLeafPage *>(split_page->GetData());
  bplus_split_page->Init(split_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(bplus_split_page);
  return bplus_split_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) { // if the old node is the root, populate a new root
    page_id_t new_root_page_id;
    auto new_root_page = buffer_pool_manager_->NewPage(new_root_page_id);
    // if there are not enough pages to allocate a new root page
    if (new_root_page == nullptr) {
      throw std::runtime_error("out of memory");
    }
    auto bplus_new_root_page = reinterpret_cast<BPlusTreeInternalPage *>(new_root_page->GetData());
    // initialize the page as a new root page 
    bplus_new_root_page->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    
    // fetch the pages that match the old node and new node first (because we are going to update them)
    // // 这里其实有点勉强，因为在进入者一层调用之前我们已经unpin了这些pages
    // // 但是接下来来要修改他们的parent page id，然后又要unpin，所以这里fetch相当于重新pin一下
    // buffer_pool_manager_->FetchPage(old_node->GetPageId());
    // buffer_pool_manager_->FetchPage(new_node->GetPageId());

    // update the old_node's and new_node's parent page id
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    
    // update the root page id stored in the IndexRootsPage
    auto index_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
    index_page->Update(index_id_, new_root_page_id);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);

    // update the new root's contents, and then unpin those pages, notice here the new root will not split
    bplus_new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    root_page_id_ = bplus_new_root_page->GetPageId();
    return ;
  }
  else {
    auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto bplus_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
    bplus_parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // if the size of the page after insertion exceeds the max size, recursively perform split
    if (bplus_parent_page->GetSize() > internal_max_size_) {
      auto bplus_split_page = Split(bplus_parent_page, transaction);
      // unpin the page that has been updated
      buffer_pool_manager_->UnpinPage(bplus_parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(bplus_split_page->GetPageId(), true);
      // recursively insert into its parent
      InsertIntoParent(bplus_parent_page, bplus_split_page->KeyAt(0), bplus_split_page);
      return ;
    }
    buffer_pool_manager_->UnpinPage(bplus_parent_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()) { // if the current tree is empty, return immediately
    return;
  }
  
  auto leaf_page = FindLeafPage(key, root_page_id_);
  auto bplus_leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(leaf_page->GetData());

  // record the first value of leaf page
  int64_t old_first_value = bplus_leaf_page->ValueAt(0).Get();
  GenericKey* old_first_key = new GenericKey;
  memcpy(old_first_key, bplus_leaf_page->KeyAt(0), processor_.GetKeySize());

  // if there isn't such a record to be removed, unpin the clean page
  if (bplus_leaf_page->RemoveAndDeleteRecord(key, processor_) == -1) buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  // check if the removed record is the first value of the leaf page
  if (old_first_value != bplus_leaf_page->ValueAt(0).Get()) { // if true, change the corresponding key in the upper layer
    GenericKey* new_key = bplus_leaf_page->KeyAt(0);

    int index;
    auto bplus_parent_page = FindParentPage(old_first_key, bplus_leaf_page->GetPageId(), index);
    if (index != -1) { // if the index is valid
      bplus_parent_page->SetKeyAt(index, new_key);
      buffer_pool_manager_->UnpinPage(bplus_parent_page->GetPageId(), true);
    }
  }


  // if the size is invalid, amend it
  if (bplus_leaf_page->GetSize() < bplus_leaf_page->GetMinSize()) {
    // if merge/coalesce happens, delete the page
    // if (CoalesceOrRedistribute(bplus_leaf_page, transaction)) buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    CoalesceOrRedistribute(bplus_leaf_page, transaction);
    // else buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
  else { // unpin the dirty page
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
  
  // LOG(INFO) << "afetr amending bplus leaf page value at 0 is " << bplus_leaf_page->ValueAt(0).Get();
  // LOG(INFO) << "afetr amending bplus leaf page value at 1 is " << bplus_leaf_page->ValueAt(1).Get();
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if(node->GetParentPageId() == INVALID_PAGE_ID) { // deal with the root
    if (node->GetSize() >= 2) { // if there are more than 2 values, the root is valid
      return false;
    }
    else { // if there is only one value left, update the root
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      UpdateRootPageId();
      return true;
    }
  }
  // ASSERT(node->GetParentPageId() != INVALID_PAGE_ID, "root is empty"); // make sure removing records won't empty the root
  
  // if the size of the node is valid, unpin the page
  if (node->GetSize() >= node->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return false;
  }

  // find the parent node of the current node
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto bplus_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
  // find the sibling of the current node
  page_id_t sib_page_id;
  // find the sibling's index
  int index = bplus_parent_page->ValueIndex(node->GetPageId());
  if (index == 0) { // if the current node is the first child
    sib_page_id = bplus_parent_page->ValueAt(index+1);
  }
  else { // if the current node is not the first child, i'll just consider the pre node as its sibling for the sake of convenience
    sib_page_id = bplus_parent_page->ValueAt(index-1);
  }
  auto sib_page = buffer_pool_manager_->FetchPage(sib_page_id);
  auto bplus_sib_page = reinterpret_cast<N *>(sib_page->GetData());
  
  // first check whether we can merge the current page with its sibling
  if (bplus_sib_page->GetSize() + node->GetSize() <= node->GetMaxSize()) { // merge two nodes
    // if (Coalesce(bplus_sib_page, node, bplus_parent_page, index, transaction)) { // if recursive deletion happens, delete the parent page
      // buffer_pool_manager_->UnpinPage(parent_page->GetPageId());
      // buffer_pool_manager_->DeletePage(parent_page->GetPageId());
    // }
    Coalesce(bplus_sib_page, node, bplus_parent_page, index, transaction); // merge two nodes
    // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(sib_page_id, true);
    return true;
  }
  else { // redistribute
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    Redistribute(bplus_sib_page, node, index);
    return false;
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index == 0) { // if the current node is at index 0, merge the node at index 1
    // move all of the contents of the node to its neighbor
    neighbor_node->MoveAllTo(node);
    // remove the node at index 1
    parent->Remove(1);
    // delete the physical page and unpin the node and the neighbor node
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    
  }
  else { // merge the node in front of the current node
    node->MoveAllTo(neighbor_node);

    // remove the current node
    parent->Remove(index);

    // delete the physical page
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(node->GetPageId());

  }
  
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index == 0) {
    GenericKey* middle_key = parent->KeyAt(parent->ValueIndex(neighbor_node->GetPageId()));
    ASSERT(parent->ValueIndex(neighbor_node->GetPageId()) != -1, "middle_key calculation error");
    neighbor_node->MoveAllTo(node, middle_key, buffer_pool_manager_);

    // remove the node at index 1
    parent->Remove(1);

    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
  }
  else {
    GenericKey* middle_key = parent->KeyAt(parent->ValueIndex(node->GetPageId()));
    ASSERT(parent->ValueIndex(node->GetPageId()) != -1, "middle_key calculation error");
    node->MoveAllTo(neighbor_node, middle_key, buffer_pool_manager_);
    
    parent->Remove(index);

    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(node->GetPageId());
  }
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  // find the parent node of the current node
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto bplus_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
  if (index == 0) {
    int middle_key_idx = bplus_parent_page->ValueIndex(neighbor_node->GetPageId());
    ASSERT(middle_key_idx != -1, "middle key calculation error");

    // calculate the key that is going to be the new middle key
    GenericKey* new_middle_key = neighbor_node->KeyAt(1);

    // update the parent node's middle key
    bplus_parent_page->SetKeyAt(middle_key_idx, new_middle_key);

    // redistribute the key
    neighbor_node->MoveFirstToEndOf(node);
  }
  else {
    int middle_key_idx = bplus_parent_page->ValueIndex(neighbor_node->GetPageId());
    ASSERT(middle_key_idx != -1, "middle key calculation error");

    // calculate the key that is going to be the new middle key
    GenericKey* new_middle_key = neighbor_node->KeyAt(neighbor_node->GetSize()-1);

    // redistribute the key
    neighbor_node->MoveLastToFrontOf(node);

    // update the parent node's middle key
    bplus_parent_page->SetKeyAt(middle_key_idx, new_middle_key);
  }

  buffer_pool_manager_->UnpinPage(bplus_parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  // find the parent node of the current node
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto bplus_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());

  // redistribute the key-value pairs
  if (index == 0) {
    int middle_key_idx = bplus_parent_page->ValueIndex(neighbor_node->GetPageId());
    GenericKey* middle_key = bplus_parent_page->KeyAt(middle_key_idx);  
    ASSERT(middle_key_idx != -1, "middle_key calculation error");
    
    // calculate the key that going to be the new middle key
    GenericKey* new_middle_key = neighbor_node->KeyAt(1);

    // update the parent node's middle key
    bplus_parent_page->SetKeyAt(middle_key_idx, new_middle_key);

    // redistribute the key
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);

  }
  else {
    int middle_key_idx = bplus_parent_page->ValueIndex(node->GetPageId());
    GenericKey* middle_key = bplus_parent_page->KeyAt(middle_key_idx);
    ASSERT(middle_key_idx != -1, "middle key calculation error");
    
    // calculate the key that is going to be the new middle key
    GenericKey* new_middle_key = neighbor_node->KeyAt(neighbor_node->GetSize()-1);

    // redistribute the key
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);

    // update the parent node's middle key
    bplus_parent_page->SetKeyAt(middle_key_idx, new_middle_key);
  }

  buffer_pool_manager_->UnpinPage(bplus_parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto page = FindLeafPage(new GenericKey(), root_page_id_, true);
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto page = FindLeafPage(key, root_page_id_); 
  auto bplus_leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
  int index = bplus_leaf_page->KeyIndex(key, processor_);
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  auto cur_page = buffer_pool_manager_->FetchPage(page_id);
  if (reinterpret_cast<BPlusTreePage *>(cur_page->GetData())->IsLeafPage()) { // if the current page is a leaf page
    // notice that here we do not unpin the page, because our return type is page*, which means we still need to use it in the function that calls this function
    return cur_page;
  }
  else { // if the current page is a internal page
    auto bplus_cur_page =  reinterpret_cast<BPlusTreeInternalPage *>(cur_page->GetData());
    // before go on to find the page, unpin the curpage fisrt
    buffer_pool_manager_->UnpinPage(page_id, false);
    if (leftMost) { // if the left most is true, find in the left most page
      return FindLeafPage(key, bplus_cur_page->ValueAt(0), leftMost);
    }
    else { // else find the page according to the key
      return FindLeafPage(key, bplus_cur_page->Lookup(key, processor_), leftMost);
    }
  }
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  // 服了为什么这个函数放在最后，我前面写的时候没看到，直接手动更新了。
  // 目前这个函数只在删除的过程中要更新root的时候调用了
      auto node = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
      root_page_id_ = node->ValueAt(0);
      // update the root page id stored in the IndexRootsPage
      auto index_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
      index_page->Update(index_id_, root_page_id_);
      buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);

      // set the new root page's parent page id to InvalidPageId, and delete the old root page
      auto bplus_new_root_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
      bplus_new_root_page->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
      buffer_pool_manager_->DeletePage(node->GetPageId());
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}


// helper fucntion
BPlusTreeInternalPage *BPlusTree::FindParentPage(const GenericKey *old_key, page_id_t page_id, int& index) {
  // check if we enter an invalid page
  ASSERT(page_id != INVALID_PAGE_ID, "unexpected error");

  auto bplus_cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  if (bplus_cur_page->IsLeafPage()) { // if the current page is a leaf page, go up
    buffer_pool_manager_->UnpinPage(page_id, false);
    return FindParentPage(old_key, bplus_cur_page->GetParentPageId(), index);
  }
  else { // if the current page is an inernal page, check if it has the old key
    auto bplus_internal_cur_page = reinterpret_cast<BPlusTreeInternalPage *>(bplus_cur_page);
      int start = 1, mid, end = bplus_internal_cur_page->GetSize()-1;
      while (start <= end) {
        mid = (start + end) / 2;
        if (processor_.CompareKeys(old_key, bplus_internal_cur_page->KeyAt(mid)) == -1) {
          end = mid - 1;
        }
        else if (processor_.CompareKeys(old_key, bplus_internal_cur_page->KeyAt(mid)) == 1) {
          start = mid + 1;
        }
        else { // if successfully find the key in the page, return this page (and unpin it later in the porcedure 'remove')
          index = mid;
          return bplus_internal_cur_page;
        }
      }
      // if there isn't the old key, find in the upper layer
      buffer_pool_manager_->UnpinPage(page_id, false);
      if(bplus_internal_cur_page->GetParentPageId() != INVALID_PAGE_ID) return FindParentPage(old_key, bplus_internal_cur_page->GetParentPageId(), index);
      else { // this means the key is the left most key of the bplustree
        index = -1; // set the index to an invalid number
        return bplus_internal_cur_page; // though this is useless
      }
  }
}