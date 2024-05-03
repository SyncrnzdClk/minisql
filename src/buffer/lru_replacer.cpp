#include "buffer/lru_replacer.h"
#include <algorithm>

LRUReplacer::LRUReplacer(size_t num_pages) {
  for (frame_id_t i = 0; i < num_pages; i++) {
    irreplaceList.push_back(i);
  }
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (this->Size() == 0) return false;
  else {
    *frame_id = replaceList.front();
    irreplaceList.push_back(*frame_id);
    replaceList.remove(replaceList.front());
  }
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto it = std::find(replaceList.begin(), replaceList.end(), frame_id);
  if (it != replaceList.end()) { 
    replaceList.erase(it);
    irreplaceList.push_back(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto it = std::find(irreplaceList.begin(), irreplaceList.end(), frame_id);
  if (it != irreplaceList.end()) {
    irreplaceList.erase(it);
    replaceList.push_back(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return replaceList.size();
}

// used for debug
size_t LRUReplacer::TotalSize() {
  return replaceList.size() + irreplaceList.size();
}