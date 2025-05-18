// myminiob/src/sql/stmt/drop_table_stmt.cpp
#include "drop_table_stmt.h"
#include "storage/db/db.h"
#include "common/log/log.h"

#include "sql/stmt/stmt.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/drop_table_stmt.h"
RC DropTableStmt::create(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt)
{
     if (db == nullptr ||  drop_table.relation_name == "") {  //droptable==nullptr
        LOG_WARN("invalid argument. db=%p, drop_table=%p, table_name=%p", 
                db, drop_table, drop_table.relation_name);
        return RC::INVALID_ARGUMENT;
    }

   string table_name = drop_table.relation_name;
    if (db->find_table(table_name) == nullptr) {
        LOG_WARN("table %s does not exist", table_name);
        return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    stmt = new DropTableStmt(db, table_name.c_str());
    if (stmt == nullptr) {
        LOG_WARN("failed to create DropTableStmt");
        return RC::NOMEM;
    }
    return RC::SUCCESS;
}