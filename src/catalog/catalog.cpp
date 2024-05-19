#include "catalog/catalog.h"

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
      TableMetadata* table_meta = TableMetadata::Create(it->first, "", INVALID_PAGE_ID, nullptr);
      TableMetadata::DeserializeFrom(tablePage->GetData(), table_meta);
      
      TableHeap* table_heap = TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager, lock_manager);
      table_info->Init(table_meta, table_heap);
      buffer_pool_manager->UnpinPage(it->second, false);

      // set the data about the table in the catalog manager
      table_names_.emplace(table_meta->GetTableName(), table_meta->GetTableId());
      tables_.emplace(table_meta->GetTableName(), table_info);

    }

    // set the data about the index
    for (auto it = catalog_meta_->index_meta_pages_.begin(); it != catalog_meta_->index_meta_pages_.end(); it++) {
      // fetch the page that contains the data about the index, and deserialize the data into index_meta
      auto indexPage = buffer_pool_manager->FetchPage(it->second);
      IndexInfo* index_info = IndexInfo::Create();
      // here we create a new index_meta (with invalid contents, just to allocate memory) and then deserialize the date into it
      IndexMetadata* index_meta = IndexMetadata::Create(it->first, "", -1, vector<uint32_t>());
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
  
  // for every table, we will assign a new page to manage it
  page_id_t page_id; 
  
  // if there isn't enough page, return DB_FAILED
  Page* table_meta_page = buffer_pool_manager_->NewPage(page_id);
  if (table_meta_page == nullptr) return DB_FAILED;

  // create a new table heap
  table_id_t table_id = next_table_id_++;
  TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, nullptr, nullptr);
  TableMetadata* table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema);
  
  // serialize the table_meta into the table_meta_page
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page->GetPageId(), true);
  table_info->Init(table_meta, table_heap);

  // update the information in catalog manager
  table_names_.emplace(table_name, table_id);
  tables_.emplace(table_id, table_info);
  
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
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
  return DB_FAILED;
}