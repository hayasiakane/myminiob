// myminiob/src/executor/drop_table_executor.h

#pragma once

#include "common/sys/rc.h"



class SQLStageEvent;



/**
 * @brief 执行器
 * @ingroup Executor
 */


class DropTableExecutor 
{
public:
    DropTableExecutor() = default;
    virtual ~DropTableExecutor() = default;

    RC execute(SQLStageEvent *sql_event);
};