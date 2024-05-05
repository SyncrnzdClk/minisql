#include "record/schema.h"
#include "gtest/gtest.h"
#include <vector>

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t serialzeSize = 0;
  
  MACH_WRITE_UINT32(buf+serialzeSize, SCHEMA_MAGIC_NUM);
  serialzeSize += sizeof(uint32_t);

  // serialize the number of columns first
  MACH_WRITE_UINT32(buf+serialzeSize, GetColumnCount());
  serialzeSize += sizeof(uint32_t);

  // serialize the content of the columns
  for (auto columns : columns_) {
    columns->SerializeTo(buf+serialzeSize);
    serialzeSize += columns->GetSerializedSize();
  }
  return serialzeSize;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t serializeSize = 0;
  serializeSize += sizeof(uint32_t); // schema_magic_num
  for (auto column : columns_) {
    serializeSize += column->GetSerializedSize();
  }
  return serializeSize;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t serializeSize = 0;
  uint32_t magicNum = 0;
  magicNum = MACH_READ_UINT32(buf + serializeSize);
  EXPECT_EQ(magicNum, SCHEMA_MAGIC_NUM);
  serializeSize += sizeof(uint32_t);

  // deserialize the number of columns
  uint32_t columnNum;
  columnNum = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  std::vector<Column*> columns; 

  for (int i = 0; i < columnNum; i++) {
    Column* column;
    column->DeserializeFrom(buf + serializeSize, column);
    serializeSize += column->GetSerializedSize();
    columns.push_back(column);
  }
  schema = new Schema(columns);
  return serializeSize;
}