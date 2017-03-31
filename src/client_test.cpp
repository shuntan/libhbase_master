/*
 * client_test.cpp
 *
 *  Created on: 2016年11月26日
 *      Author: shuntan
 */
#include "hbase_helper.h"

// 创建了一个表名  "table"
// 测试IP 127.0.0.1:9090

extern "C" int main(int argc, char* argv[])
{
	//CHbaseClientHelper::Ignore_Log(); //忽略日志

	//////////////////////////////INSERT//////////////////////////////////////////////
	std::vector<hbase::TRow> rows;
	hbase::TRow row(hbase::PUT);
	row.set_rowkey("row1");
	row.add_column_value("family1", "qualifier1","value1");
	row.add_column_value("family1", "qualifier2", "value2");
	row.add_column_value("family2", "qualifier1", "value1");
	rows.push_back(row);
    row.reset(hbase::PUT);
	row.set_rowkey("row2");
	row.add_column_value("family1", "qualifier1", "value1");
	rows.push_back(row);

	if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").put_multi("table", rows))
	{
		printf(" insert failed! \n");
	}
	else
	{
		printf(" insert success! \n");
	}

	rows.clear();

	////////////////////////////////EXSIST/////////////////////////////////////////////
    row.reset();
	row.set_rowkey("row1");
	row.add_column("family1","qualifier1");
	if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").exist("table", row))
	{
		printf(" don't exist ! \n");
	}
	else
	{
		printf(" exist ! \n");
	}
	row.reset();

	/////////////////////////////////DELETE//////////////////////////////////////////////
	row.set_rowkey("row1");
    row.add_column("family1","qualifier1");
	if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").erase("table", row))
	{
		printf(" delete failed! \n");
	}
	else
	{
		printf(" delete success! \n");
	}
    row.reset();

	////////////////////////////////////GET/////////////////////////////////////////////////
	row.set_rowkey("row1");
	if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").get("table", row))
	{
		printf(" get failed! \n");
	}
	else
	{
		printf(" get success! \n");
	}
    row.reset();

	////////////////////////////////////SCANGET////////////////////////////////////////////
	if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").get_by_scan("table", "row1", "row10", rows, 10))
	{
		printf(" get scan failed! \n");
	}
	else
	{
		printf(" get scan success! \n");
	}
    row.reset();

	////////////////////////////////////////APPEND/////////////////////////////////////////
    row.reset(hbase::PUT);
	row.set_rowkey("row1");
	row.add_column_value("family1", "qualifier2", "valueadd");
	if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").append("table", row))
	{
		printf(" append failed! \n");
	}
	else
	{
		printf(" append success! \n");
	}

	///////////////////////////////////////INCREMENT//////////////////////////////////////////
	if(!hbase::CHbaseClientHelper::get_singleton("127.0.0.1:9090").increment("table", "row2", "family1", "qualifier1", 233))
	{
		printf(" Increment failed! \n");
	}
	else
	{
		printf(" Increment success! \n");
	}

	return 0;
}



