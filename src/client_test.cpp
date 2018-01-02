/*
 * client_test.cpp
 *
 *  Created on: 2016年11月26日
 *      Author: shuntan
 */
#include <iostream>
#include "hbase_client.h"

// 创建了一个表名  "table_test"
// 测试IP 10.223.25.102:9091

inline static void print(const std::string& row_key, const hbase::thrift2::HBRow& row)
{
    for(hbase::thrift2::HBRow::const_iterator iter_ = row.begin(); iter_ != row.end(); iter_++)
    {
        std::cout << "key:"<< row_key << ",family-column:" << iter_->first
                << ",value:" << iter_->second.m_value << ",time:" << iter_->second.m_timestamp << "\n" <<std::endl;
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
        client = new hbase::thrift2::CHBaseClient("10.223.25.102:9090");
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
        delete client;
        return 0;
    }
	//////////////////////////////INSERT//////////////////////////////////////////////
    hbase::thrift2::HBTable rows;
    rows["key1"]["info:name"] = "shuntan";
    rows["key1"]["info:love"] = "cooking";
    rows["key1"]["info:addr"] = "shenzhen";
    rows["key2"]["info:name"] = "jayyi";
    rows["key2"]["info:love"] = "game";
    rows["key2"]["info:addr"] = "guangzhou";

	try
	{
	    client->put("table_test", rows);
	    printf(" insert success! \n");

	}
	catch(hbase::thrift2::CHBaseException& ex)
	{
	    printf("ex:%s\n", ex.str().c_str());
	}


	////////////////////////////////EXSIST/////////////////////////////////////////////
	hbase::thrift2::HBRow row;
    row["info:name"];

    try
    {
        if(!client->exist("table_test", "key1", row))
        {
            printf(" don't exist ! \n");
        }
        else
        {
            printf(" exist ! \n");
        }
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	/////////////////////////////////DELETE//////////////////////////////////////////////
    row.clear();
    row["info:name"];
    try
    {
        client->erase("table_test", "key2", row);
        printf(" delete success! \n");
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	////////////////////////////////////GET/////////////////////////////////////////////////
    row.clear();
    row["info:name"];
    row["info:love"];
    row["info:addr"];
    row["info:yy"];
    row["info:age"];

	try
	{
	    client->get("table_test", "key1", row);
	    printf(" get success! \n");
	    print("key1", row);

	    // 在hbase中的int型需要大小端转换一下。
	    printf("bswap16:(%ld) size(%u)\n", bswap_64(*(int64_t*)(row["info:age"].m_value.c_str())),row["info:age"].m_value.size());
	}
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	////////////////////////////////////SCANGET////////////////////////////////////////////
    rows.clear();
    row.clear();
    row["info:name"];
    row["info:love"];

    try
    {
        client->get("table_test", "key1", "key10", row, rows, 10);
        printf(" get scan success! \n");
        print(rows);
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }


	////////////////////////////////////////APPEND/////////////////////////////////////////
    row.clear();
    row["info:name"] = "-NULL";
    row["info:love"] = "-****";

	try
	{
	    hbase::thrift2::HBRow result = client->append("table_test", "key1", row);
	    printf(" append success! result-1[%s], result-2[%s]\n", result["info:name"].m_value.c_str(), result["info:love"].m_value.c_str());
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	///////////////////////////////////////INCREMENT//////////////////////////////////////////
    try
    {
        std::string result = client->increment("table_test", "key1", "info", "age", "23");
        printf(" Increment success, result[%s]! \n", result.c_str());
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

    //////////////////////////////////////CHECKANDPUT////////////////////////////////////////
    row.clear();
    row["info:JJ"] = "15";
    try
    {
        bool res = client->check_and_put("table_test", "key1", "info", "JJ", "", row);
        printf(" check_and_put success, result[%u]! \n", res ? 1 : 0);
    }
    catch(hbase::thrift2::CHBaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	return 0;
}



