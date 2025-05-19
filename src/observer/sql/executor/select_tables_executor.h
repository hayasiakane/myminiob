#pragma once

#include "common/sys/rc.h"



class SQLStageEvent;
#include "sql/executor/sql_result.h"


/**
 * @brief 执行器
 * @ingroup Executor
 */


class SelectTableExecutor 
{
public:
    SelectTableExecutor() = default;
    virtual ~SelectTableExecutor() = default;

    RC execute(SQLStageEvent *sql_event);
};