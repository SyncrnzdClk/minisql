#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // check if the page is full
  if (this->page_allocated_ >= MAX_CHARS * 8) return false;

  // check if the next_free_page_ is valid
  if (IsPageFree(this->next_free_page_)) {
    // set the place to be taken
    SetPageTakenLow(this->next_free_page_ / 8, this->next_free_page_ % 8);
    // update the page_offset
    page_offset = this->next_free_page_;
    
    // update next_free_page_
    this->next_free_page_ = (this->next_free_page_ + 1) % (MAX_CHARS * 8);
    // LOG(INFO) << "Allocate Page " << page_offset << " at " << this->next_free_page_ << "th bit";

    ++(this->page_allocated_);
  }
  // if the next_free_page_ is invalid, find the valid next_free_page
  else {
    // update next_free_page_
    FindNextFreePage();

    // LOG(INFO) << "Find Next Free Page " << this->next_free_page_;
    
    // set the place to be taken
    SetPageTakenLow(this->next_free_page_ / 8, this->next_free_page_ % 8);

    // update the page_offset
    page_offset = this->next_free_page_;
    
    // update next_free_page_
    this->next_free_page_ = (this->next_free_page_ + 1) % (MAX_CHARS * 8);

    ++(this->page_allocated_);
  }
  // if (flag) LOG(INFO) <<  "page_al = " << this->page_allocated_ << " MAX_CHAR * 8 = " << MAX_CHARS * 8;

  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // check if the page is empty
  if (IsPageFree(page_offset) || (this->page_allocated_ == 0)) return false;


  // set the place to be free
  this->bytes[page_offset / 8] &= ~('\01' << page_offset % 8);

  // update the page_offset
  this->page_allocated_--;
  // this->next_free_page_ = page_offset;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // LOG(INFO) << "page_offet = " << page_offset;
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  // check if the place is free or not
  if (this->bytes[byte_index] & ('\01' << bit_index)) return false;
  return true;
}

template <size_t PageSize>
void BitmapPage<PageSize>::FindNextFreePage() {
  // find the valid next_free_page
  while (!IsPageFree(this->next_free_page_)) {
    this->next_free_page_++;

    // if the next_free_page_ exceeds the MAX_CHARS
    if (next_free_page_ >= MAX_CHARS * 8) {
      this->next_free_page_ %= (MAX_CHARS * 8);
    }
  }
}

template <size_t PageSize>
void BitmapPage<PageSize>::SetPageTakenLow(uint32_t byte_index, uint8_t bit_index) {
  this->bytes[byte_index] |= ('\01' << bit_index);
} 

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;