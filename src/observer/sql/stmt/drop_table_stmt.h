#pragma once

#include "common/lang/string.h"
#include "common/lang/vector.h"
#include "sql/stmt/stmt.h"
#include "sql/parser/parse_defs.h"

class Db;

class DropTableStmt : public Stmt
{
public:
    DropTableStmt(Db *db, const char *table_name) : db_(db), table_name_(table_name) {}
    virtual ~DropTableStmt() = default;

    StmtType type() const override { return StmtType::DROP_TABLE; }
    const char *table_name() const { return table_name_.c_str(); }
    Db *db() const { return db_; }

public:
    static RC create(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt);

private:
    Db *db_ = nullptr;
    std::string table_name_;
};