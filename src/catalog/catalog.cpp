#include "catalog/catalog.h"
#include "common/macros.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  return 4 * 3 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
  }
  else {
    // fetch the catalog page from the memory and deserialize the data
    auto page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    // set the data about table
    for (auto it = catalog_meta_->table_meta_pages_.begin(); it != catalog_meta_->table_meta_pages_.end(); it++) {
      
      // fetch the page that contains the data about the table, and deserialize the data into table_meta
      auto tablePage = buffer_pool_manager->FetchPage(it->second);
      TableInfo* table_info = TableInfo::Create();
      // here we create a table_meta first (with invalid contents, just to allocate memory), and then deserialize the data into it
      // TableMetadata* table_meta = TableMetadata::Create(it->first, "", INVALID_PAGE_ID, nullptr);
      TableMetadata* table_meta = nullptr;
      TableMetadata::DeserializeFrom(tablePage->GetData(), table_meta);

      TableHeap* table_heap = TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager, lock_manager);
      table_info->Init(table_meta, table_heap);
      buffer_pool_manager->UnpinPage(it->second, false);

      // set the data about the table in the catalog manager
      table_names_.emplace(table_meta->GetTableName(), table_meta->GetTableId());
      tables_.emplace(table_meta->GetTableId(), table_info);

    }

    // set the data about the index
    for (auto it = catalog_meta_->index_meta_pages_.begin(); it != catalog_meta_->index_meta_pages_.end(); it++) {
      // fetch the page that contains the data about the index, and deserialize the data into index_meta
      auto indexPage = buffer_pool_manager->FetchPage(it->second);
      IndexInfo* index_info = IndexInfo::Create();

      IndexMetadata* index_meta = nullptr;
      IndexMetadata::DeserializeFrom(indexPage->GetData(), index_meta);

      // initialize the index_info
      ASSERT(tables_.find(index_meta->GetTableId()) != tables_.end(), "the table does not exist");      
      TableInfo* table_info = tables_.find(index_meta->GetTableId())->second;
      index_info->Init(index_meta, table_info, buffer_pool_manager);
      buffer_pool_manager->UnpinPage(it->second, false);

      // set the map for indexes: table_name->index_name->indexes
      // first we check whether this index belongs to some table which is already in the map
      auto index_name_map_it = index_names_.find(table_info->GetTableName());
      if (index_name_map_it == index_names_.end()) { // if the table is not in the index_name map, insert a new <table, map<index_name, index_id>> into it 
        unordered_map<std::string, index_id_t> index_name_id_map;
        index_name_id_map.emplace(index_meta->GetIndexName(), index_meta->GetIndexId());
        index_names_.emplace(table_info->GetTableName(), index_name_id_map);
      }
      else { // if the table is already in the index_name map, insert the pair <index_name, index_id> into the map in the second position
        index_name_map_it->second.emplace(index_meta->GetIndexName(), index_meta->GetIndexId());
      }
      indexes_.emplace(index_meta->GetIndexId(), index_info);
    }
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
    
  }
//    ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  
  // first check whether the table is already exist
  if (table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  
  // create table info object and copy schema
  table_info = TableInfo::Create();
  auto table_schema = TableSchema::DeepCopySchema(schema);

  // create table heap
  auto table_heap = TableHeap::Create(buffer_pool_manager_, table_schema, txn, log_manager_, lock_manager_);

  // create table metadata
  auto table_id = next_table_id_.fetch_add(1);
  auto table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), table_schema);

  // init table info
  table_info->Init(table_meta, table_heap);

  // emplace the map
  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_name, table_id);
  index_names_.emplace(table_name, std::unordered_map<std::string, index_id_t>{});

  // write table metadata to table metadata page
  auto table_meta_page_id = INVALID_PAGE_ID;
  auto page = buffer_pool_manager_->NewPage(table_meta_page_id);
  ASSERT(page != nullptr, "page allocation error.");
  table_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);

  // write catalog metadata to catalog meatadata page
  catalog_meta_->table_meta_pages_.emplace(table_id, table_meta_page_id);
  ASSERT(DB_SUCCESS == FlushCatalogMetaPage(), "failed to flush catalog meta page."); 

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");

  // first find the table id in the table_names
  auto it_table_name = table_names_.find(table_name);
  // if there wasn't such a table, return table_not_exist 
  if (it_table_name == table_names_.end()) return DB_TABLE_NOT_EXIST;

  // normally if the table name exists in table_names_, the table info should exist in tables_
  ASSERT(tables_.find(it_table_name->second) != tables_.end(), "unexpected error");
  table_info = tables_.find(it_table_name->second)->second;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  
  if (tables_.size() == 0) return DB_TABLE_NOT_EXIST;

  ASSERT(tables_.size() == table_names_.size(), "unexpected error");
  // iterate through the whole tables_ to get all the tables
  for (auto it = tables_.begin(); it != tables_.end(); it++) {
    // push the table info to the back of the tables
    tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  
  // check whether the table exist
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;
  // check if the index already exist
  std::unordered_map<std::string, index_id_t>& indexes_in_table = index_names_.at(table_name);
  if (indexes_in_table.count(index_name) != 0) return DB_INDEX_ALREADY_EXIST;

  // initialize index info
  // 1. initialize index meta data
  // 1.1 construct key map
  std::vector<uint32_t> keymap; 
  TableInfo* host_table = tables_.at(table_names_.at(table_name));
  Schema* index_schema = host_table->GetSchema();
  for (auto index_key : index_keys) {
    uint32_t key;
    if (index_schema->GetColumnIndex(index_key, key) != DB_COLUMN_NAME_NOT_EXIST) {
      keymap.push_back(key);
    }
    else {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }

  // update the index information in the corresponding table
  index_id_t index_id = next_index_id_.fetch_add(1);
  indexes_in_table.emplace(index_name, index_id);

  // 1.2 initialize the meta data
  IndexMetadata* index_meta = IndexMetadata::Create(index_id, index_name, table_names_[table_name], keymap);

  // 2.1 initialize the index info
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, host_table, buffer_pool_manager_);

  // 2.2 insert the index info information into the indexes_
  indexes_.emplace(index_id, index_info);

  // 3 write index metadata to index metadata page
  auto index_meta_page_id = INVALID_PAGE_ID;
  auto page = buffer_pool_manager_->NewPage(index_meta_page_id);
  ASSERT(page != nullptr, "page allocation error.");
  index_meta->SerializeTo(page->GetData());

  // 4 write the catalog metadata to catalog metadata page
  catalog_meta_->index_meta_pages_.emplace(index_id, index_meta_page_id);
  ASSERT(DB_SUCCESS == FlushCatalogMetaPage(), "failed to flush catalog meta page.");

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");

  // first check if the table and the index exist
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;
  if (index_names_.count(table_name) == 0) return DB_INDEX_NOT_FOUND;

  const std::unordered_map<std::string, index_id_t>& indexes_in_table = index_names_.at(table_name); // notice here we use at instead of [], because it is a conts function, while [] permits insertion
  if (indexes_in_table.count(index_name) == 0) return DB_INDEX_NOT_FOUND;

  // get the index id and get the index info accrodingly
  const index_id_t& index_id = indexes_in_table.at(index_name);
  index_info = indexes_.at(index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");

  // first check if the table and idnexes exist
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;
  if (index_names_.count(table_name) == 0) return DB_INDEX_NOT_FOUND;

  // get the indexes in the given table
  const unordered_map<std::string, index_id_t>& index_in_table = index_names_.at(table_name);

  // insert the index info into the indexes
  for (const auto& target_index : index_in_table) {
    const index_id_t& index_id = target_index.second;
    indexes.push_back(indexes_.at(index_id));
  }

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  // first check if the table exists
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;

  // remove the information in table_names_ and tables_
  table_id_t table_id = table_names_.at(table_name);
  TableInfo*& table_info = tables_.at(table_id);
  // get the table heap and free it
  TableHeap* table_heap = table_info->GetTableHeap();
  table_heap->FreeTableHeap();
  // delete table info
  delete table_info;
  tables_.erase(table_id);
  table_names_.erase(table_name);

  if (index_names_.count(table_name) > 0) { // if there are some indexes on the table
    // remove the information in the idnex_names_ and indexes_
    std::unordered_map<std::string, index_id_t>& index_in_table = index_names_.at(table_name);
    for (auto& target_index : index_in_table) { // iterate through all the pairs in the map to delete the index info and erase the record in the indexes_
      IndexInfo*& index_info = indexes_.at(target_index.second);
      Index* index = index_info->GetIndex();
      // destroy the whole index
      index->Destroy();
      delete index_info;

      // delete the information about the index in the index_names_ and indexes_
      indexes_.erase(target_index.second);
      index_names_.erase(target_index.first);

      // erase the index information in catalog meta
      catalog_meta_->index_meta_pages_.erase(target_index.second);
      indexes_.erase(target_index.second);
    }
    index_names_.erase(table_name);
  }
  
  // erase the table information in catalog meta
  catalog_meta_->table_meta_pages_.erase(table_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  
  // check if the table exists
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;
  
  // check if the index exists
  if (index_names_.count(index_name) == 0) return DB_INDEX_NOT_FOUND;

  // find the index and delete it
  std::unordered_map<std::string, index_id_t>& index_in_table = index_names_.at(table_name);
  index_id_t index_id = index_in_table.at(index_name);
  IndexInfo*& index_info = indexes_.at(index_id);
  Index* index = index_info->GetIndex();
  index->Destroy();
  delete index_info;
  indexes_.erase(index_id);
  index_in_table.erase(index_name);
  

  // erase the information about the index in the index_meta_pages_
  catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  // buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  // buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_SUCCESS;
  else return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");

  auto it = tables_.find(table_id);
  if (it != tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it->second;

  return DB_SUCCESS;
}