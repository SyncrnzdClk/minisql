#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <vector>
#include <mutex>
#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * ClockReplacer implements the Clock replacement policy.
 */
class ClockReplacer : public Replacer {
public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

  size_t TotalSize() override;

private:
  size_t num_pages_; // Maximum number of pages
  vector<bool> reference_bits_; // Reference bits for clock algorithm
  vector<frame_id_t> frames_; // Frame ids stored in the replacer
  size_t hand_; // Current position of the clock hand
};

#endif  // MINISQL_LRU_REPLACER_H
