/*
 * demo.cpp
 *
 *  Created on: 2017年11月27日
 *      Author: Administrator
 */
#include <iostream>
#include "hbase_client.h"

inline static void print(const std::string& row_key, const hbase::thrift2::HBRow& row)
{
    for(hbase::thrift2::HBRow::const_iterator iter_ = row.begin(); iter_ != row.end(); iter_++)
    {
        std::cout << "key:"<< row_key << ",family-column:" << iter_->first
                << ",value:" <<  iter_->second.m_value << ",time:" << iter_->second.m_timestamp << "\n" <<std::endl;
        if(iter_->first == "cf1:money")
            printf("bswap16:(%ld) size(%u)\n", bswap_64(*(int64_t*)(iter_->second.m_value.c_str())),iter_->second.m_value.size());
    }
}

inline static void print(const hbase::thrift2::HBTable& row_list)
{
    for(hbase::thrift2::HBTable::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
    {
        print(iter->first,iter->second);
    }
}

extern "C" int main(int argc, char* argv[])
{
    //CHbaseClientHelper::Ignore_Log(); //忽略日志
    hbase::thrift2::CHBaseClient* client = NULL;

    try
    {
        client = new hbase::thrift2::CHBaseClient("10.240.133.104:9090");
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
        delete client;
        return 0;
    }

    hbase::thrift2::HBRow row;
    row["cf1"];


    try
    {
        client->get("gongyi_user:user_summary", "06392555", row);
        print("06392555", row);

    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }
}
