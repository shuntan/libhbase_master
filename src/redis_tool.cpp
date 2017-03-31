/*
 * redis_tool.cpp
 *
 *  Created on: 2017年3月27日
 *      Author: Administrator
 */
#include "hbase_helper.h"

static std::string to_dictionary_number(int32_t num, int8_t size_limit = 10)
{
    std::string str;
    std::ostringstream stream;
    stream<<num;
    int8_t num_size = stream.str().size();

    if(size_limit > num_size)
    {
        int8_t repair_size = size_limit - num_size;
        for(int8_t i = 0; i < repair_size; i ++)
        {
            str += "0";
        }
    }

    return (str + stream.str());
}

extern "C" int main()
{
    try
    {

        std::vector<hbase::TRow> rows;
        for (int i = 0; i < 100; i++)
        {
            hbase::TRow row(hbase::PUT);
            row.set_rowkey(to_dictionary_number(i));
            row.add_column_value("rank", "id","1");
            row.add_column_value("rank", "steps", "99999");
            rows.push_back(row);
        }

        if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").put_multi("shuntan", rows))
        {
            printf(" puts failed! \n");
        }
        else
        {
            printf(" puts success! \n");
            rows.clear();
        }

        if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").get_by_scan("shuntan", "0000000003", "0000000087", rows, 10))
        {
            printf(" gets failed! \n");
        }
        else
        {
            printf(" gets success! \n");
        }

        printf("show meta!\n");
        for(std::vector<hbase::TRow>::const_iterator iter = rows.begin(); iter != rows.end(); iter++)
        {
            const std::string& row_key = iter->get_rowkey();
            for(std::vector<hbase::TCell>::const_iterator iter_ = iter->get_columns().begin(); iter_ != iter->get_columns().end(); iter_++)
            {
                printf("row_key:(%s),%s:%s:(%s)time_stamp:(%llu)\n", row_key.c_str(), iter_->m_family.c_str(), iter_->m_qualifier.c_str(), iter_->m_value.c_str(), iter_->m_time_stamp);
            }
        }

    }
    catch(hbase::CHbaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

    return 0;
}



