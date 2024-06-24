#include "buffer/clock_replacer.h"
#include "gtest/gtest.h"

TEST(ClockReplacerTest, SampleTest) {
  ClockReplacer clock_replacer(5);

  // Unpin pages to fill the replacer
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  EXPECT_EQ(5, clock_replacer.Size());

  // Victim should follow the clock policy
  frame_id_t value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Pin some pages
  clock_replacer.Pin(4);
  clock_replacer.Pin(5);
  EXPECT_EQ(0, clock_replacer.Size());

  // Unpin again
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(7);
  EXPECT_EQ(4, clock_replacer.Size());

  // Victim selection should now be in order considering unpinned pages
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(7, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);

  EXPECT_EQ(0, clock_replacer.Size());
}
