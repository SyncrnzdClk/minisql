#include "record/row.h"
#include <vector>
#include <iostream>

// appendix for iterating two containers at the same time
template <typename T, typename U>
auto zip(const T& a, const U& b) {
    return std::vector<std::tuple<typename T::value_type, typename U::value_type>>(a.begin(), a.end(), b.begin());
}
/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t serializeSize = 0;
  for (auto field : fields_) {
    field->SerializeTo(buf+serializeSize);
    serializeSize += field->GetSerializedSize();
  }

  return serializeSize;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t serializeSize = 0;
  for (auto column : schema->GetColumns()) {
    Field* field;
    field->DeserializeFrom(buf+serializeSize, column->GetType(), &field, false);
    serializeSize += field->GetSerializedSize();
    fields_.push_back(field);
  }
  return serializeSize;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t serializeSize = 0;
  for (auto field : fields_) {
    serializeSize += field->GetSerializedSize();
  }
  return serializeSize;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
