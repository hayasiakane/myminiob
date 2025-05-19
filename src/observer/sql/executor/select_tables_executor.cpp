// 文件路径：myminiob/src/observer/sql/executor/select_table_executor.cpp
#include "sql/executor/select_tables_executor.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/common/condition_filter.h"
#include "storage/table/heap_table_engine.h"
#include "storage/table/lsm_table_engine.h"
#include "sql/expr/composite_tuple.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/executor/sql_result.h"
#include "sql/operator/physical_operator.h"
#include "storage/record/record_scanner.h"
#include "sql/expr/tuple.h"
#include "common/log/log.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "session/session.h"
#include "sql/stmt/stmt.h"
// 新增：TupleConditionFilter 类，适配 Tuple 过滤
class TupleConditionFilter {
public:
    TupleConditionFilter() = default;
    virtual ~TupleConditionFilter() = default;
    
    // 过滤 CompositeTuple
    virtual bool filter(CompositeTuple* tuple) const = 0;
};

// 新增：DefaultTupleConditionFilter 类，实现单个条件过滤
class DefaultTupleConditionFilter : public TupleConditionFilter {
public:
    DefaultTupleConditionFilter(const FilterUnit* filter_unit) 
        : filter_unit_(filter_unit) {}
    
    bool filter(CompositeTuple* tuple) const override {
        if (!filter_unit_) return true;
        
        const FilterObj& left_obj = filter_unit_->left();
        const FilterObj& right_obj = filter_unit_->right();
        CompOp comp = filter_unit_->comp();
        
        // 计算左右值
        Value left_val, right_val;
        RC rc;
        
        // 处理左操作数
        if (left_obj.is_attr) {
            // 通过 Tuple 获取字段值
            rc = tuple->find_cell(
                TupleCellSpec(left_obj.field.table_name(), left_obj.field.field_name()), 
                left_val
            );
            if (rc != RC::SUCCESS) {
                LOG_WARN("Failed to evaluate left expression: %s", strrc(rc));
                return false;
            }
        } else {
            left_val = left_obj.value;
        }
        
        // 处理右操作数
        if (right_obj.is_attr) {
            // 通过 Tuple 获取字段值
            rc = tuple->find_cell(
                TupleCellSpec(right_obj.field.table_name(), right_obj.field.field_name()), 
                right_val
            );
            if (rc != RC::SUCCESS) {
                LOG_WARN("Failed to evaluate right expression: %s", strrc(rc));
                return false;
            }
        } else {
            right_val = right_obj.value;
        }
        
        // 类型转换
        if (left_val.attr_type() != right_val.attr_type()) {
            if (left_val.cast_to(left_val,right_val.attr_type(),left_val) != RC::SUCCESS) {
                // 尝试将右值转换为左值类型
                if (left_val.cast_to(right_val,left_val.attr_type(),right_val) != RC::SUCCESS) {
                    LOG_WARN("Incompatible types in condition: %d vs %d", 
                             left_val.attr_type(), right_val.attr_type());
                    return false;
                }
            }
        }
        
        // 根据比较运算符判断条件结果
        bool condition_result = false;
        int cmp_result = left_val.compare(right_val);
        switch (comp) {
        case CompOp::EQUAL_TO: condition_result = (cmp_result == 0); break;
        case CompOp::NOT_EQUAL: condition_result = (cmp_result != 0); break;
        case CompOp::LESS_THAN: condition_result = (cmp_result < 0); break;
        case CompOp::GREAT_THAN: condition_result = (cmp_result > 0); break;
        case CompOp::LESS_EQUAL: condition_result = (cmp_result <= 0); break;
        case CompOp::GREAT_EQUAL: condition_result = (cmp_result >= 0); break;
        default:
            LOG_WARN("Unsupported comparison operator: %d", comp);
            return false;
        }
        
        return condition_result;
    }
    
private:
    const FilterUnit* filter_unit_ = nullptr;
};

// 新增：CompositeTupleConditionFilter 类，组合多个条件
class CompositeTupleConditionFilter : public TupleConditionFilter {
public:
    CompositeTupleConditionFilter() = default;
    ~CompositeTupleConditionFilter() override {
        for (auto filter : filters_) {
            delete filter;
        }
    }
    
    void add_filter(TupleConditionFilter* filter) {
        if (filter) filters_.push_back(filter);
    }
    
    bool filter(CompositeTuple* tuple) const override {
        for (auto filter : filters_) {
            if (!filter->filter(tuple)) {
                return false;
            }
        }
        return true;
    }
    
private:
    std::vector<TupleConditionFilter*> filters_;
};

// 笛卡尔积操作符（保持之前的实现不变）
class CartesianProductOperator : public PhysicalOperator {
public:
    CartesianProductOperator(const std::vector<Table*> &tables, FilterStmt *filter_stmt) 
        : tables_(tables), filter_stmt_(filter_stmt) {}
    
     PhysicalOperatorType type() const override {
        return PhysicalOperatorType::CARTESIAN_PRODUCT;
    }
    //virtual PhysicalOperatorType type() override  ;
    // virtual Tuple *current_tuple() { return nullptr; }

    // virtual RC tuple_schema(TupleSchema &schema) const { return RC::UNIMPLEMENTED; }

    // void add_child(unique_ptr<PhysicalOperator> oper) { children_.emplace_back(std::move(oper)); }

    // virtual string name() const;
    // virtual string param() const;
        
    RC open(Trx *trx) override {
        scanners_.resize(tables_.size());
        for (size_t i = 0; i < tables_.size(); ++i) {
            RecordScanner *scanner = nullptr;
            RC rc;
            if (tables_[i]->table_meta().storage_engine() == StorageEngine::HEAP) {
                HeapTableEngine *engine = static_cast<HeapTableEngine*>(tables_[i]->engine());
                rc = engine->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY);
            } else if (tables_[i]->table_meta().storage_engine() == StorageEngine::LSM) {
                LsmTableEngine *engine = static_cast<LsmTableEngine*>(tables_[i]->engine());
                rc = engine->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY);
            }
            if (rc != RC::SUCCESS) {
                LOG_ERROR("Failed to open scanner for table %s", tables_[i]->name());
                return rc;
            }
            scanners_[i] = scanner;
        }
        current_records_.resize(tables_.size());
        
        // 初始化 Tuple 条件过滤器
        init_tuple_condition_filter();
        
        return RC::SUCCESS;
    }

    RC next(Tuple *&tuple)  {
        if (done_) {
            return RC::RECORD_EOF;
        }

        while (true) {
            if (!generate_next_combination()) {
                done_ = true;
                return RC::RECORD_EOF;
            }

            CompositeTuple *composite_tuple = new CompositeTuple();
            for (size_t i = 0; i < tables_.size(); ++i) {
                RowTuple *record_tuple = new RowTuple();
                record_tuple->set_record(&current_records_[i]);
                record_tuple->set_schema(tables_[i], tables_[i]->table_meta().field_metas());
                composite_tuple->add_tuple(std::unique_ptr<Tuple>(record_tuple));
            }

            // 使用新的 TupleConditionFilter 进行过滤
            if (tuple_condition_filter_ == nullptr || tuple_condition_filter_->filter(composite_tuple)) {
                tuple = composite_tuple;
                return RC::SUCCESS;
            }

            delete composite_tuple;
        }
    }

    RC close() override {
        for (RecordScanner *scanner : scanners_) {
            scanner->close_scan();
            delete scanner;
        }
        delete tuple_condition_filter_;  // 释放过滤器资源
        return RC::SUCCESS;
    }

private:
    // 初始化 Tuple 条件过滤器
    void init_tuple_condition_filter() {
        if (filter_stmt_ == nullptr) {
            tuple_condition_filter_ = nullptr;
            return;
        }
        
        CompositeTupleConditionFilter* composite_filter = new CompositeTupleConditionFilter();
        
        const std::vector<FilterUnit*> &filter_units = filter_stmt_->filter_units();
        for (FilterUnit *unit : filter_units) {
            if (!unit) continue;
            
            DefaultTupleConditionFilter* filter = new DefaultTupleConditionFilter(unit);
            composite_filter->add_filter(filter);
        }
        
        tuple_condition_filter_ = composite_filter;
    }

    bool generate_next_combination() {
        // 保持原有实现不变
        size_t i = scanners_.size() - 1;
        while (i < scanners_.size()) {
            if (scanners_[i]->next(current_records_[i]) == RC::SUCCESS) {
                return true;
            }

            if (i == 0) {
                return false;
            }

            scanners_[i]->close_scan();
            delete scanners_[i];
            RecordScanner *scanner = nullptr;
            RC rc;
            if (tables_[i]->table_meta().storage_engine() == StorageEngine::HEAP) {
                HeapTableEngine *engine = static_cast<HeapTableEngine*>(tables_[i]->engine());
                rc = engine->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY);
            } else if (tables_[i]->table_meta().storage_engine() == StorageEngine::LSM) {
                LsmTableEngine *engine = static_cast<LsmTableEngine*>(tables_[i]->engine());
                rc = engine->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY);
            }
            if (rc != RC::SUCCESS) {
                LOG_ERROR("Failed to reopen scanner for table %s", tables_[i]->name());
                return false;
            }
            scanners_[i] = scanner;
            --i;
        }
        return false;
    }

    std::vector<Table*> tables_;
    std::vector<RecordScanner*> scanners_;
    std::vector<Record> current_records_;
    FilterStmt *filter_stmt_;
    bool done_ = false;
    
    // 使用新的 TupleConditionFilter
    TupleConditionFilter* tuple_condition_filter_ = nullptr;
};

// 新增：SelectTableExecutor::execute 方法实现
RC SelectTableExecutor::execute( SQLStageEvent *sql_event)
{
    
    // 获取当前 SQL 语句的 Stmt 对象
    Stmt *stmt = sql_event->stmt();
    if (stmt == nullptr) {
        LOG_WARN("stmt is null");
        return RC::INTERNAL;
    }

    // 检查是否为 SELECT 语句
    if (stmt->type() != StmtType::SELECT) {
        LOG_WARN("stmt is not a SELECT statement");
        return RC::INTERNAL;
    }

    // 将 Stmt 转换为 SelectStmt
    SelectStmt *select_stmt = static_cast<SelectStmt*>(stmt);

    // 获取查询涉及的表
    const std::vector<Table*> &tables = select_stmt->tables();
    if (tables.empty()) {
        LOG_WARN("no tables in select stmt");
        return RC::INTERNAL;
    }

    // 获取过滤条件
    FilterStmt *filter_stmt = select_stmt->filter_stmt();

    // 创建并执行笛卡尔积操作符
    CartesianProductOperator cartesian_product(tables, filter_stmt);
    RC rc = cartesian_product.open(nullptr);
    if (rc != RC::SUCCESS) {
        LOG_ERROR("failed to open cartesian product operator: %s", strrc(rc));
        return rc;
    }

    // 创建输出模式
    TupleSchema output_schema;
    const std::vector<std::unique_ptr<Expression>> &expressions = select_stmt->query_expressions();
    for (const auto &expr : expressions) {
        FieldExpr *field_expr = dynamic_cast<FieldExpr*>(expr.get());
        if (!field_expr) {
            LOG_WARN("expression is not a field expression");
            return RC::INTERNAL;
        }
        output_schema.append_cell(field_expr->field().table_name(), field_expr->field().field_name());
    }

    // 创建结果集
    SimpleTupleSet tuple_set;
    tuple_set.set_schema(output_schema);

    // 执行查询并收集结果
    Tuple* tuple = nullptr;
    while ((rc = cartesian_product.next(tuple)) == RC::SUCCESS) {
        std::unique_ptr<Tuple> owned_tuple(tuple);
        auto output_tuple = std::make_unique<ProjectTuple>();

        
        // 从 output_schema 中提取字段信息
        std::vector<std::unique_ptr<Expression>> expressions;
        for (int i = 0; i < output_schema.cell_num(); i++) {
            const TupleCellSpec &spec = output_schema.cell_at(i);
            const char *table_name = spec.table_name();
            const char *field_name = spec.field_name();
            
            Db *db = sql_event->session_event()->session()->get_current_db();
            // 查找对应的 Table 对象
            Table *table = db->find_table(table_name);
            if (!table) {
                LOG_WARN("table not found: %s", table_name);
                return RC::SCHEMA_TABLE_NOT_EXIST;
            }
            
            // 创建 FieldExpr 表达式
            const FieldMeta *field_meta = table->table_meta().field(field_name);
            if (!field_meta) {
                LOG_WARN("field not found: %s.%s", table_name, field_name);
                return RC::SCHEMA_FIELD_NOT_EXIST;
            }
            
            auto expr = std::make_unique<FieldExpr>(table, field_meta);
            expressions.push_back(std::move(expr));
        }
        
        // 设置投影表达式和源元组
        output_tuple->set_expressions(std::move(expressions));
        output_tuple->set_tuple(tuple);
        
        tuple_set.add(std::move(output_tuple));
    }

    // 关闭操作符
    cartesian_product.close();

    // 创建自定义操作符并设置到 SqlResult
    auto oper = std::make_unique<SimpleTupleSetOperator>(
    std::make_unique<SimpleTupleSet>(std::move(tuple_set))
   );
    sql_event->session_event()->sql_result()->set_operator(std::move(oper));
    sql_event->session_event()->sql_result()->set_tuple_schema(output_schema);  // 设置元组模式
    return RC::SUCCESS;
}