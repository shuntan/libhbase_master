/*
 * client_test.cpp
 *
 *  Created on: 2016年11月26日
 *      Author: shuntan
 */
#include "hbase_helper.h"
#include <iostream>

// 创建了一个表名  "table_test"
// 测试IP 127.0.0.1:9090

inline static void print(const std::string& row_key, const hbase::HBRow& row)
{
    for(hbase::HBRow::const_iterator iter_ = row.begin(); iter_ != row.end(); iter_++)
    {
        std::cout << "key:"<< row_key << ",family-column:" << iter_->first
                << ",value:" << iter_->second.m_value << ",time:" << iter_->second.m_time_stamp << "\n" <<std::endl;
    }
}

inline static void print(const hbase::HBTable& row_list)
{
    for(hbase::HBTable::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
    {
        print(iter->first,iter->second);
    }
}

extern "C" int main(int argc, char* argv[])
{
	//CHbaseClientHelper::Ignore_Log(); //忽略日志

	//////////////////////////////INSERT//////////////////////////////////////////////
    hbase::HBTable rows;
    rows["key1"]["info:name"] = "shuntan";
    rows["key1"]["info:love"] = "cooking";
    rows["key1"]["info:addr"] = "shenzhen";
    rows["key2"]["info:name"] = "jayyi";
    rows["key2"]["info:love"] = "game";
    rows["key2"]["info:addr"] = "guangzhou";

	try
	{
	    hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").put_multi("table_test", rows);
	    printf(" insert success! \n");

	}
	catch(hbase::CHbaseException& ex)
	{
	    printf("ex:%s\n", ex.str().c_str());
	}


	////////////////////////////////EXSIST/////////////////////////////////////////////
	hbase::HBRow row;
    row["info:name"];

    try
    {
        if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").exist("table_test", "key1", row))
        {
            printf(" don't exist ! \n");
        }
        else
        {
            printf(" exist ! \n");
        }
    }
    catch(hbase::CHbaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	/////////////////////////////////DELETE//////////////////////////////////////////////
    row.clear();
    row["info:name"];
    try
    {
        hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").erase("table_test", "key2", row);
        printf(" delete success! \n");
    }
    catch(hbase::CHbaseException& ex)
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
	    hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").get("table_test", "key1", row);
	    printf(" get success! \n");
	    print("key1", row);

	    // 在hbase中的int型需要大小端转换一下。
	    printf("bswap16:(%ld) size(%u)\n", bswap_64(*(int64_t*)(row["info:age"].m_value.c_str())),row["info:age"].m_value.size());
	}
    catch(hbase::CHbaseException& ex)
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
        hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").get_by_scan("table_test", "key1", "key10", row, rows, 10);
        printf(" get scan success! \n");
        print(rows);
    }
    catch(hbase::CHbaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }


	////////////////////////////////////////APPEND/////////////////////////////////////////
    row.clear();
    row["info:name"] = "-NULL";
    row["info:love"] = "-****";

	try
	{
	    hbase::HBRow result = hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").append_multi("table_test", "key1", row);
	    printf(" append success! result-1[%s], result-2[%s]\n", result["info:name"].m_value.c_str(), result["info:love"].m_value.c_str());
    }
    catch(hbase::CHbaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	///////////////////////////////////////INCREMENT//////////////////////////////////////////
    try
    {
        int64_t result = hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").increment("table_test", "key1", "info", "age", 23);
        printf(" Increment success, result[%lld]! \n", result);
    }
    catch(hbase::CHbaseException& ex)
    {
        printf("ex:%s\n", ex.str().c_str());
    }

	return 0;
}



