#include "buffer/clock_replacer.h"
#include "glog/logging.h"

ClockReplacer::ClockReplacer(size_t num_pages)
    : num_pages_(num_pages), reference_bits_(num_pages, false), frames_(num_pages, -1), hand_(0) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  while (true) {
    if (frames_[hand_] != -1) {
      if (reference_bits_[hand_]) {
        // Give second chance
        reference_bits_[hand_] = false;
      } else {
        // Evict this frame
        *frame_id = frames_[hand_];
        frames_[hand_] = -1;
        return true;
      }
    }
    hand_ = (hand_ + 1) % num_pages_;
  }
  return false; // No victim found
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  for (size_t i = 0; i < num_pages_; ++i) {
    if (frames_[i] == frame_id) {
      frames_[i] = -1;
      reference_bits_[i] = false;
      break;
    }
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  for (size_t i = 0; i < num_pages_; ++i) {
    if (frames_[i] == -1) {
      frames_[i] = frame_id;
      reference_bits_[i] = true;
      return;
    }
  }
}

size_t ClockReplacer::Size() {
  size_t size = 0;
  for (size_t i = 0; i < num_pages_; ++i) {
    if (frames_[i] != -1) {
      ++size;
    }
  }
  return size;
}

size_t ClockReplacer::TotalSize() {
  return num_pages_;
}
