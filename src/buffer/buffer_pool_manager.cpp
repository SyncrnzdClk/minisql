#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

#include <algorithm>

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id, bool flag) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  
  // find the page
  // auto it = std::find(page_table_.begin(), page_table_.end(), page_id);
  // if successfully find it, pin it.
  if (page_table_.count(page_id) > 0) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    // return the page
    return &(pages_[frame_id]);
  }

  // if the page is not in the page table
  // check if the free_list is not empty
  if (free_list_.size() > 0) {
    // update the members
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].is_dirty_ = false; // the new page is clean
    page_table_.insert(make_pair(page_id, frame_id));
    return &(pages_[frame_id]);
  }
  // check if the replacer has pages to be replaced
  else if (replacer_->Size() > 0){
    frame_id_t frame_id;
    if (replacer_->Victim(&frame_id)) {
      Page* replacePage = &(pages_[frame_id]);
      
      // if the page to be replaced is dirty, write it back to the disk
      if (replacePage->IsDirty()) {
        disk_manager_->WritePage(replacePage->page_id_, replacePage->data_);
        replacePage->is_dirty_ = false; // the new page is clean
      }

      // remove the replacePage from the page table
      page_table_.erase(page_table_.find(replacePage->page_id_));
      
      // read the new page in the disk and update the members
      disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
      replacer_->Pin(frame_id);
      pages_[frame_id].pin_count_ = 1;
      pages_[frame_id].page_id_ = page_id;
      page_table_.insert(make_pair(page_id, frame_id));
      return &(pages_[frame_id]);
    }
  }
  // if no page is able to be replaced
  return nullptr;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  if (!free_list_.empty()) { // if the free list still has place
    page_id = AllocatePage();
    if (page_id != INVALID_PAGE_ID) {
      frame_id_t frame_id = free_list_.front();
      free_list_.pop_front();
      replacer_->Pin(frame_id); // still have problem here
      pages_[frame_id].pin_count_ = 1;
      pages_[frame_id].page_id_ = page_id;
      pages_[frame_id].is_dirty_ = false;
      pages_[frame_id].ResetMemory(); // the new page is unused
      page_table_.insert(make_pair(page_id, frame_id));
      return &(pages_[frame_id]);
    }
  }
  else if (replacer_->Size() > 0){ // if the lru still has pages to be replaced
    frame_id_t frame_id;
    if (replacer_->Victim(&frame_id)) {
      Page* replacePage = &(pages_[frame_id]);
      // if the page to be replaced is dirty, write it back to the disk
      if (replacePage->IsDirty()) {
        disk_manager_->WritePage(replacePage->page_id_, replacePage->data_);
        replacePage->is_dirty_ = false; // the new page is clean
      }
      // remove the replacePage from the page table
      page_table_.erase(page_table_.find(replacePage->page_id_));
      // allocate the new page
      page_id = AllocatePage();
      if (page_id != INVALID_PAGE_ID) { // if there are free pages in the disk
        pages_[frame_id].ResetMemory();
        // replacer_->Pin(frame_id);
        pages_[frame_id].pin_count_ = 1;
        pages_[frame_id].page_id_ = page_id;
        page_table_.insert(make_pair(page_id, frame_id));
        return &(pages_[frame_id]);
      }
    }
  }
  // if there are no more free pages to be allocated
  page_id = INVALID_PAGE_ID;
  return nullptr;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  // find the page
  // auto it = std::find(page_table_.begin(), page_table_.end(), page_id); 
  frame_id_t frame_id;
  if (page_table_.count(page_id) <= 0) { // if P does not exist, return true
    DeallocatePage(page_id);
    return true;
  }
  else { // if P exist
    frame_id = page_table_[page_id];
    Page* delPage = &(pages_[frame_id]);
    if (delPage->pin_count_ > 0) return false; // if the pin count is not 0
    
    // if the page can be deleted
    page_table_.erase(page_table_.find(page_id));
    
    delPage->ResetMemory();
    delPage->pin_count_ = 0;
    delPage->page_id_ = INVALID_PAGE_ID;
    delPage->is_dirty_ = false;
    
    // add it to the free_list
    free_list_.push_back(frame_id);
    DeallocatePage(page_id);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // auto it = std::find(page_table_.begin(), page_table_.end(), page_id); 
  // LOG(INFO) << "page_id = " << page_id << "page_table_.count(page_id) = " << page_table_.count(page_id);
  if (page_table_.count(page_id) == 0) return false; // if the page does not exist
  frame_id_t frame_id = page_table_[page_id];
  Page* target = &(pages_[frame_id]);
  if (--target->pin_count_ == 0) { // if no more procedure pin this page, add it to the replaceList
    replacer_->Unpin(frame_id);
  }  
  if (is_dirty) target->is_dirty_ = true; 
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  // auto it = std::find(page_table_.begin(), page_table_.end(), page_id);
  if (page_table_.count(page_id) == 0) return false; // the page does not exist
  frame_id_t frame_id = page_table_[page_id];
  Page* target = &(pages_[frame_id]);
  disk_manager_->WritePage(page_id, target->data_);
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}