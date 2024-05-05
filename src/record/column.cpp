#include "record/column.h"

#include "glog/logging.h"

#include "gtest/gtest.h"
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}



/**
 * magic_num -> name(length -> data) -> type -> len_ -> table_ind -> nullable -> unique
*/

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t serializeSize = 0;

  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  serializeSize += sizeof(uint32_t);
  
  MACH_WRITE_UINT32(buf + serializeSize, name_.length());
  serializeSize += sizeof(uint32_t);

  MACH_WRITE_STRING(buf + serializeSize, name_);
  serializeSize += sizeof(name_.length());

  MACH_WRITE_TO(uint32_t, buf + serializeSize, type_);
  serializeSize += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf + serializeSize, len_);
  serializeSize += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf + serializeSize, table_ind_);
  serializeSize += sizeof(uint32_t);

  MACH_WRITE_TO(bool, buf + serializeSize, nullable_);
  serializeSize += sizeof(bool);

  MACH_WRITE_TO(bool, buf + serializeSize, unique_);
  serializeSize += sizeof(bool);

  return serializeSize;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return MACH_STR_SERIALIZED_SIZE(name_) + 4 * sizeof(uint32_t) + 2 * sizeof(bool);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset;
  
  uint32_t magicNum = MACH_READ_UINT32(buf);
  offset += sizeof(uint32_t);
  EXPECT_EQ(magicNum, COLUMN_MAGIC_NUM);
  
  uint32_t nameLength = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  
  char *charname = new char[nameLength];
  memcpy(charname, buf+offset, nameLength);
  offset += nameLength;
  std::string column_name(charname);

  TypeId type = (TypeId)MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  uint32_t length = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  uint32_t index = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  bool nullable = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);

  bool unique = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);

 if (type == kTypeChar) {
    column = new Column(column_name, type, length, index, nullable, unique);
  }
  else {
    column = new Column(column_name, type, index, nullable, unique);
  } 
  delete[] charname;
  return offset;
}
