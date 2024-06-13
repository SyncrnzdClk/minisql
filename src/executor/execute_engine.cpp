#include "executor/execute_engine.h"

#include <dirent.h>
#include <parser/syntax_tree_printer.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "../../test/execution/executor_test_util.h"
#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
  int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }

  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  closedir(dir);
}
std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *table_info = nullptr;
  DBStorageEngine* cur_db_engine = dbs_[current_db_];

  // check if the table already exists
  if (cur_db_engine->catalog_mgr_->GetTable(table_name, table_info) != DB_TABLE_NOT_EXIST) {
    return DB_TABLE_ALREADY_EXIST;
  }

  // construct a series of structures to store information
  vector<string> col_names;
  vector<TypeId> col_types;
  vector<bool> col_unique;
  vector<int> col_lens;
  vector<string> primary_key_cols;

  // get the columns
  auto columns = ast->child_->next_;
  // get the column information
  int col_id = 0;
  for (auto col = columns->child_; col != nullptr; col = col->next_) {
    if (col->type_ == kNodeColumnDefinition) { // deal with columns
      col_names.emplace_back(col->child_->val_);
      col_unique.emplace_back(col->val_ != nullptr);
      string type_name = col->child_->next_->val_;
      if (type_name == "int") {
        col_types.emplace_back(kTypeInt);
        col_lens.emplace_back(0);
      }
      else if (type_name == "char") {
        col_types.emplace_back(kTypeChar);
        int col_len = atoi(col->child_->next_->child_->val_);
        if (col_len < 0) {
          cout << "Invalid char len";
          return DB_FAILED;
        }
        col_lens.emplace_back(col_len);
      }
      else if (type_name == "float") {
        col_types.emplace_back(kTypeFloat);
        col_lens.emplace_back(0);
      }
      else {
        cout << "Invalid column type";
        return DB_FAILED;
      }
    }
    else if (col->type_ == kNodeColumnList) { // deal with primary keys
      for (auto primary_key_col = col->child_; primary_key_col != nullptr; primary_key_col = primary_key_col->next_) {
        primary_key_cols.emplace_back(primary_key_col->val_);
      }
    }
  }

  // generate columns
  vector<Column *> Columns;
  bool is_manage = false; // this variable records whether there is column with type kTypeChar, for the convenience of deallocating the memory of schema
  for (int i = 0; i < col_names.size(); i++) {
    auto it = std::find(primary_key_cols.begin(), primary_key_cols.end(), col_names[i]);
    if (it == primary_key_cols.end()) { // the colunmn is not a primary key
      if (col_types[i] != kTypeChar) { // if the column is of kind float or int
        Columns.push_back(new Column(col_names[i], col_types[i], i, false, col_unique[i]));
      }
      else {
        is_manage = true;
        Columns.push_back(new Column(col_names[i], col_types[i], col_lens[i], i, false, col_unique[i]));
      }
    }
    else { // the column is a primary key
      if (col_types[i] != kTypeChar) { // if the column is of kind float or int
        Columns.push_back(new Column(col_names[i], col_types[i], i, false, true));
      }
      else {
        is_manage = true;
        Columns.push_back(new Column(col_names[i], col_types[i], col_lens[i], i, false, true));
      }
    }
  }

  // generate schema
  Schema* schema = new Schema(Columns, is_manage);

  // call catalog manager to create table
  dberr_t e = context->GetCatalog()->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if (e != DB_SUCCESS) {
    cout << "create table error";
    return e;
  }

  // IndexInfo* index_info;
  //
  // // create index for primary keys
  // if (!primary_key_cols.empty()) {
  //   e = context->GetCatalog()->CreateIndex(table_name, table_name + "_pk_index", primary_key_cols, context->GetTransaction(), index_info, "bptree");
  // }
  // if (e != DB_SUCCESS) {
  //   cout << "create index error" << endl;
  // }
  return e;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected." << endl;
    return DB_FAILED;
  }
  // get the table name
  string table_name = ast->child_->val_;

  // check if the table exists
  TableInfo* table_info;
  dberr_t e = context->GetCatalog()->GetTable(table_name, table_info);
  if (e != DB_SUCCESS) {
    cout << "table does not exist" << endl;
    return e;
  }

  // drop the table
  e = context->GetCatalog()->DropTable(table_name);
  if (e != DB_SUCCESS) {
    std::cout << "drop table error" << endl;
  }

  // drop the indexes on the table
  vector<IndexInfo*> indexes_on_table;
  context->GetCatalog()->GetTableIndexes(table_name, indexes_on_table);
  for (auto index_on_table : indexes_on_table) {
    e = context->GetCatalog()->DropIndex(table_name, index_on_table->GetIndexName());
    if (e != DB_SUCCESS) {
      cout << "drop index on table error." << endl;
      return e;
    }
  }
  cout << "drop table " << table_name << " success" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected." << endl;
    return DB_FAILED;
  }

  // get the information about the tables
  vector<TableInfo*> table_infos;
  if (context->GetCatalog()->GetTables(table_infos) != DB_SUCCESS) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }

  // get all of the indexes matching the tables
  map<string, vector<IndexInfo *>> table_index_map;
  for (auto table_info : table_infos) {
    vector<IndexInfo *> index_infos;
    string table_name = table_info->GetTableName();
    if (context->GetCatalog()->GetTableIndexes(table_name, index_infos) != DB_SUCCESS) {
      cout << "Empty set (0.00 sec)" << endl;
      return DB_FAILED;
    }
    table_index_map.emplace(table_name, index_infos);
  }

  // show the information about the indexes
  for (auto table_index_pair : table_index_map) {
    cout << "we have these indexes on table " << table_index_pair.first << endl;
    for (auto index_info : table_index_pair.second) {
      cout << index_info->GetIndexName() << "  on columns: ";
      for (auto col : index_info->GetIndexKeySchema()->GetColumns()) {
        cout << col->GetName() << " ";
      }
      cout << endl;
    }
  }
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected." << endl;
    return DB_FAILED;
  }

  // get the index name
  string index_name = ast->child_->val_;
  // get the table name
  string table_name = ast->child_->next_->val_;
  // get the column names
  vector<string> col_names;
  for (auto col = ast->child_->next_->next_->child_; col != nullptr; col = col->next_) {
    col_names.emplace_back(col->val_);
  }

  // get the table info
  TableInfo* table_info;
  dberr_t e = context->GetCatalog()->GetTable(table_name, table_info);
  if (e != DB_SUCCESS) {
    cout << "get table error." << endl;
    return e;
  }

  // create index
  IndexInfo* index_info;
  e = context->GetCatalog()->CreateIndex(table_name, index_name, col_names, context->GetTransaction(), index_info, "bptree");
  if (e != DB_SUCCESS) {
    cout << "create index error";
    return e;
  }

  // insert records into the index
  for (auto row_iter = table_info->GetTableHeap()->Begin(context->GetTransaction()); row_iter != table_info->GetTableHeap()->End(); ++row_iter) {
    RowId rid = row_iter->GetRowId();
    vector<Field> fields;
    for (auto col : index_info->GetIndexKeySchema()->GetColumns()) {
      fields.emplace_back(*(row_iter->GetField(col->GetTableInd())));
    }
    Row row(fields);
    e = index_info->GetIndex()->InsertEntry(row, rid, context->GetTransaction());
    if (e != DB_SUCCESS) {
      cout << "insert entry error." << endl;
      return e;
    }
  }

    // output the information
  cout << "successfully create index " << index_name << " on table " << table_name << " on column(s) ";
  for (auto col_name : col_names) {
    cout << col_name << " ";
  }
  cout << endl;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected." << endl;
    return DB_FAILED;
  }

  //  get the index name
  string index_name = ast->child_->val_;
  IndexInfo* index_info;

  // get the table that holds this index
  string table_name;
  vector<TableInfo*> table_infos;
  context->GetCatalog()->GetTables(table_infos);
  for (auto table_info : table_infos) {
    if (context->GetCatalog()->GetIndex(table_info->GetTableName(), index_name, index_info) == DB_SUCCESS) { // if successfully find the index, drop it
      dberr_t e = context->GetCatalog()->DropIndex(table_info->GetTableName(), index_name);
      if (e == DB_SUCCESS) {
        cout << "successfully drop the index.";
        return DB_SUCCESS;
      }
      else {
        cout << "drop index error.";
        return e;
      }
    }
  }

  cout << "index not found." << endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name(ast->child_->val_);
  ifstream file(file_name, ios::in);
  // Read in the commands in the file into the vector.
  int cnt = 0;
  char ch;
  const int buf_size = 1024;
  char cmd[buf_size];
  memset(cmd, 0, buf_size);
  if (file.is_open()) {
    while (file.get(ch)) {
      // Judge if a whole command is gotten.
      cmd[cnt++] = ch;
      if (ch == ';') {
        file.get(ch);  // Get the '\n' after ';'.

        YY_BUFFER_STATE bp = yy_scan_string(cmd);
        yy_switch_to_buffer(bp);
        MinisqlParserInit();
        yyparse();
        auto result = Execute(MinisqlGetParserRootNode());
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();
        ExecuteInformation(result);
        if (result == DB_QUIT) {
          break;
        }
        memset(cmd, 0, buf_size);
        cnt = 0;
      }
    }
    file.close();
  }


  // Process the commands one by one.
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}
