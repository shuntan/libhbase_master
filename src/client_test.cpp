/*
 * client_test.cpp
 *
 *  Created on: 2016年11月26日
 *      Author: shuntan
 */
#include "hbase_client_helper.h"

// 创建了一个表名  "table"
// 测试IP 127.0.0.1:9090

extern "C" int main(int argc, char* argv[])
{
	//CHbaseClientHelper::Ignore_Log(); //忽略日志

	//////////////////////////////INSERT//////////////////////////////////////////////
	std::vector<TRow> rows;
	TRow row;
	row.set_rowkey("row1");
	row.add_value("family1", "qualifier1", "value1");
	row.add_value("family1", "qualifier2", "value2");
	row.add_value("family2", "qualifier1", "value1");
	rows.push_back(row);
	row.clear();
	row.set_rowkey("row2");
	row.add_value("family1", "qualifier1", "value1");
	rows.push_back(row);

	if(!CHbaseClientHelper::Get_Singleton("127.0.0.1:9090").Insert("table", rows))
	{
		printf(" insert failed! \n");
	}
	else
	{
		printf(" insert success! \n");
	}
	row.clear();
	rows.clear();

	////////////////////////////////EXSIST/////////////////////////////////////////////
	row.set_rowkey("row1");
	row.add_qualifier("family1","qualifier1");
	if(!CHbaseClientHelper::Get_Singleton("127.0.0.1:9090").Exist("table", row))
	{
		printf(" don't exist ! \n");
	}
	else
	{
		printf(" exist ! \n");
	}
	row.clear();

	/////////////////////////////////DELETE//////////////////////////////////////////////
	row.set_rowkey("row1");
	row.add_qualifier("family1", "qualifier1");
	if(!CHbaseClientHelper::Get_Singleton("127.0.0.1:9090").Delete("table", row))
	{
		printf(" delete failed! \n");
	}
	else
	{
		printf(" delete success! \n");
	}
	row.clear();

	////////////////////////////////////GET/////////////////////////////////////////////////
	row.set_rowkey("row1");
	if(!CHbaseClientHelper::Get_Singleton("127.0.0.1:9090").Get("table", row))
	{
		printf(" get failed! \n");
	}
	else
	{
		printf(" get success! \n");
	}
	row.clear();

	////////////////////////////////////SCANGET////////////////////////////////////////////
	if(!CHbaseClientHelper::Get_Singleton("127.0.0.1:9090").Get("table", "row1", "row10", rows, 10))
	{
		printf(" get scan failed! \n");
	}
	else
	{
		printf(" get scan success! \n");
	}
	row.clear();

	////////////////////////////////////////APPEND/////////////////////////////////////////
	row.set_rowkey("row1");
	row.add_value("family1", "qualifier2", "valueadd");
	if(!CHbaseClientHelper::Get_Singleton("127.0.0.1:9090").Append("table", row))
	{
		printf(" append failed! \n");
	}
	else
	{
		printf(" append success! \n");
	}

	///////////////////////////////////////INCREMENT//////////////////////////////////////////
	if(!CHbaseClientHelper::Get_Singleton("127.0.0.1:9090").Increment("table", "row2", "family1", "qualifier1", 233))
	{
		printf(" Increment failed! \n");
	}
	else
	{
		printf(" Increment success! \n");
	}

	return 0;
}



