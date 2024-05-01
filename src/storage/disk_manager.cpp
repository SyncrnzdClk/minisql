#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage* meta = reinterpret_cast<DiskFileMetaPage*>(meta_data_);


  
  page_id_t physical_bitmap_page_id = INVALID_PAGE_ID, physical_data_page_id = INVALID_PAGE_ID;
  // find free page_id
  for (uint32_t extent_index = 0; extent_index < meta->GetExtentNums(); extent_index++) {
    if (meta->GetExtentUsedPage(extent_index) < BITMAP_SIZE) { // find free bitmap
      physical_bitmap_page_id = extent_index * (BITMAP_SIZE + 1) + 1; // calculate the physical page id of the bitmap

      // update meta data
      meta->extent_used_page_[extent_index]++;
      break;
    }
  }
  if (physical_bitmap_page_id == INVALID_PAGE_ID) {
    if (meta->GetAllocatedPages() < MAX_VALID_PAGE_ID) {
      // update meta data
      meta->num_extents_++;
      meta->extent_used_page_[meta->GetExtentNums()-1] = 1;
      physical_bitmap_page_id = (meta->GetExtentNums() - 1) * (BITMAP_SIZE + 1) + 1; // calculate the physical page id of the bitmap
    }
    else { // there isn't free page anymore
      return INVALID_PAGE_ID;
    }
  }
  
  // read the bitmap and allocate a free page
  char* bitmap_extent_data = new char[PAGE_SIZE];
  ReadPhysicalPage(physical_bitmap_page_id, bitmap_extent_data);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_extent_data);
  uint32_t bitmap_page_offset;
  if (bitmap->AllocatePage(bitmap_page_offset) == false) LOG(INFO) << "FALSE!!!!!!! " << " meta->num_allocated_pages = " << meta->num_allocated_pages_ + 1 << " FALSE!!!!!";


  // rewrite the bitmap page
  WritePhysicalPage(physical_bitmap_page_id, reinterpret_cast<char*>(bitmap));

  // calculate the physical page id of the data page
  physical_data_page_id = physical_bitmap_page_id + bitmap_page_offset + 1;

  
  // update the meta page
  meta->num_allocated_pages_++;
  page_id_t logical_page_id = physical_data_page_id - (physical_data_page_id - 1) / (BITMAP_SIZE + 1) - 2; // map physical page id to logical page id
  // LOG(INFO) << "meta->num_allocated_pages = " << meta->num_allocated_pages_;
  // LOG(INFO) << "meta->num_extents = " << meta->num_extents_;
  // LOG(INFO) << "physical_bitmap_page_id = " << physical_bitmap_page_id << " bitmap_page_offset = " << bitmap_page_offset;
  return logical_page_id;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if (IsPageFree(logical_page_id)) {
    LOG(INFO) << "heer";
    return;
  }
  else {
    DiskFileMetaPage* meta = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
    
    uint32_t bitmap_extent_offset = logical_page_id / BITMAP_SIZE;
    uint32_t  physical_bitmap_page_id = bitmap_extent_offset * (BITMAP_SIZE + 1) + 1;
    char* bitmap_extent_data = new char[PAGE_SIZE];
    ReadPhysicalPage(physical_bitmap_page_id, bitmap_extent_data);
    BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_extent_data);
    uint32_t bitmap_page_offset = logical_page_id % BITMAP_SIZE;
    bitmap->DeAllocatePage(bitmap_page_offset);

    WritePhysicalPage(physical_bitmap_page_id, reinterpret_cast<char*>(bitmap));
    LOG(INFO) << "ispagefree(logical) = " << IsPageFree(logical_page_id);
    // update the meta page
    meta->num_allocated_pages_--;
    meta->extent_used_page_[bitmap_extent_offset]--;
    delete[] bitmap_extent_data;
    
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t bitmap_extent_offset = logical_page_id / BITMAP_SIZE;
  page_id_t physical_bitmap_extent_offset = 1 + bitmap_extent_offset * (BITMAP_SIZE + 1); // calculate the physical page id of the bitmap
  char* bitmap_extent_data = new char[PAGE_SIZE]; // allocate memory for the bitmap_extent_data
  ReadPhysicalPage(physical_bitmap_extent_offset, bitmap_extent_data); // read the bitmap_extent_data from the disk
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_extent_data); // cast the bitmap_extent_data to a BitmapPage 
  bool is_free = bitmap->IsPageFree(logical_page_id % BITMAP_SIZE); // check if the page is free in the bitmap
  delete[] bitmap_extent_data;
  if (is_free) return true;
  else return false;
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return 2 + logical_page_id / BITMAP_SIZE + logical_page_id % BITMAP_SIZE + logical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}