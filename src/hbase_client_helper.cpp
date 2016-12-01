/*
 * hbase_client_helper.cpp
 *
 *  Created on: 2016年11月19日
 *      Author: shuntan
 */
#include "../../c-hbase-client/src/hbase_client_helper.h"

bool CHbaseClientHelper::m_enable_log = true;
CHbaseClientHelper::CHbaseClientHelper(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out)
{
	std::vector<std::string> host_array;
	common::split(&host_array, host_list, ",");
	for(std::vector<std::string>::const_iterator iter = host_array.begin(); iter != host_array.end(); iter++)
	{
		std::vector<std::string> ip_port;
		common::split(&ip_port, *iter, ":");
		if(ip_port.size() != 2)
			continue;

		const std::string& host_ip   = ip_port[0];
		const std::string& host_port = ip_port[1];
		CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = new CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>(host_ip, atoi(host_port.c_str()), connect_timeout, recive_timeout, send_time_out);

		unsigned int retry_times = 3;
		while(retry_times)
		{
			try
			{
				hbase_client->connect();
				m_hbase_clients.push_back(hbase_client);
				__HLOG_INFO(m_enable_log, "push back node[%s:%s] success\n", host_ip.c_str(), host_port.c_str());
				break;
			}
			catch (apache::thrift::transport::TTransportException& ex)
			{
				__HLOG_INFO(m_enable_log, "surplus try time [%u] connect hbase://%s:%s, transport(I/O) exception: (%d)(%s)",retry_times, host_ip.c_str(), host_port.c_str(), ex.getType(), ex.what());
				retry_times --;
				sleep(1);
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "connect hbase://%s:%s application exception: %s",host_ip.c_str(), host_port.c_str(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "connect hbase://%s:%s,exception: [%s].", host_ip.c_str(), host_port.c_str(), ex.what());
				break;
			}
		}

		if(m_hbase_clients.size() < 1)
		{
			__HLOG_ERROR(m_enable_log, "Hbase service list empty! \n");
	        _exit(1);
		}
	}
}

CHbaseClientHelper& CHbaseClientHelper::Get_Singleton(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out)
{
	return *Get_SingletonPtr(host_list, connect_timeout, recive_timeout, send_time_out);
}

CHbaseClientHelper* CHbaseClientHelper::Get_SingletonPtr(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out)
{
	static CHbaseClientHelper* s_singleton = NULL;
	if(!s_singleton)
	{
		s_singleton = new CHbaseClientHelper(host_list, connect_timeout, recive_timeout, send_time_out);
	}
	return s_singleton;
}

CHbaseClientHelper::~CHbaseClientHelper()
{
	for(std::vector<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >::const_iterator iter = m_hbase_clients.begin(); iter != m_hbase_clients.end(); iter++)
	{
		delete (*iter);
	}
}

void* CHbaseClientHelper::Get_Random_Service()
{
    static unsigned int factor = 0;
    std::vector<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >::size_type i = common::get_random_number(factor++, static_cast<unsigned int>(m_hbase_clients.size()));
    return m_hbase_clients[i];
}

bool CHbaseClientHelper::Reconnet(void* hbase_client)
{
	try
	{
		static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient> *>(hbase_client)->connect();
		return true;
	}
	catch (apache::thrift::transport::TTransportException& ex)
	{
		return false;
	}
}

bool CHbaseClientHelper::Exist(const std::string& table_name,  const TRow& row)
{
	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	apache::hadoop::hbase::thrift2::TGet get;

	get.__set_row(row.get_rowkey());
	std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
	for(std::vector<TCell>::const_iterator iter = row.get_cells().begin(); iter != row.get_cells().end(); iter++)
	{
	    apache::hadoop::hbase::thrift2::TColumn  family_column;
	    family_column.__set_family(iter->m_Family);
	    family_column.__set_qualifier(iter->m_Qualifier);
	    family_columns.push_back(family_column);
	}
	get.__set_columns(family_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
		try
		{
			bool bResult ;
			bResult = (*hbase_client)->exists(table_name, get);
			return bResult;
		}

	    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
	    {
	        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
	        break;
	    }

		catch (apache::thrift::transport::TTransportException& ex)
		{
			// type = 2 或者 unknow的时候才需要重连
			if ( retry_times - 1 == i)
			{
				__HLOG_ERROR(m_enable_log, "exists %s transport exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			}

			if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
			{
				hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
			}

			if(!hbase_client->is_connected())
			{
				if ( !Reconnet(hbase_client) ) break;
			}
		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(m_enable_log, "exists %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			break;
		}
		catch (apache::thrift::TException& ex)
		{
			__HLOG_ERROR(m_enable_log, "exists %s exception: %s", table_name.c_str(), ex.what());
			break;
		}
	}

	return false;
}

/////////////////////////////////////////////////////INSERT////////////////////////////////////////////////////////////////////

bool CHbaseClientHelper::Insert(const std::string& table_name, const TRow& row, TDurability::type insert_flag, uint64_t time_stamp)
{
	std::vector<TRow> multi_row;
	multi_row.push_back(row);
	return Insert(table_name, multi_row, insert_flag, time_stamp);
}

bool CHbaseClientHelper::Insert(const std::string& table_name, const std::vector<TRow>& row_list, TDurability::type insert_flag, uint64_t time_stamp)
{
	if(row_list.empty())
		return false;

	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client =static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	std::vector<apache::hadoop::hbase::thrift2::TPut> puts;
	for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
	{
		apache::hadoop::hbase::thrift2::TPut put;
		put.__set_row(iter->get_rowkey());
		if(time_stamp > 0)
		{
			put.__set_timestamp(time_stamp);
		}
		std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_columns;
		for(std::vector<TCell>::const_iterator iter_ = iter->get_cells().begin(); iter_ != iter->get_cells().end(); iter_++)
		{
		    apache::hadoop::hbase::thrift2::TColumnValue  family_column;
		    family_column.__set_family(iter_->m_Family);
		    family_column.__set_qualifier(iter_->m_Qualifier);
		    family_column.__set_value(iter_->m_Value);
		    family_columns.push_back(family_column);
		}
		put.__set_columnValues(family_columns);
		put.__set_durability(insert_flag);
		puts.push_back(put);
	}

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				if(puts.size() > 1)
					(*hbase_client)->putMultiple(table_name, puts);
				else
					(*hbase_client)->put(table_name, puts.front());
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
		        break;
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(m_enable_log,	 "put to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
				}

				if(!hbase_client->is_connected())
				{
					if ( !Reconnet(hbase_client) ) break;
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "put to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "put to %s exception: %s", table_name.c_str(), ex.what());
				break;
			}
	}

	return false;
}

///////////////////////////////////////////////////////ERASE///////////////////////////////////////////////////////////

bool CHbaseClientHelper::Delete(const std::string& table_name, const TRow& row, TDurability::type delete_flag, uint64_t time_stamp)
{
	std::vector<TRow> multi_row;
	multi_row.push_back(row);
	return Delete(table_name, multi_row, delete_flag, time_stamp);
}

bool CHbaseClientHelper::Delete(const std::string& table_name, const std::vector<TRow> row_list, TDurability::type delete_flag, uint64_t time_stamp)
{
	if(row_list.empty())
		return false;

	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	std::vector<apache::hadoop::hbase::thrift2::TDelete> dels;
	std::vector<apache::hadoop::hbase::thrift2::TDelete> dels_r;
	for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
	{
		apache::hadoop::hbase::thrift2::TDelete del;
		del.__set_row(iter->get_rowkey());
		if(time_stamp > 0)
		{
			del.__set_timestamp(time_stamp);
		}
		std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
		for(std::vector<TCell>::const_iterator iter_ = iter->get_cells().begin(); iter_ != iter->get_cells().end(); iter_++)
		{
			    apache::hadoop::hbase::thrift2::TColumn  family_column;
			    family_column.__set_family(iter_->m_Family);
			    family_column.__set_qualifier(iter_->m_Qualifier);
			    family_columns.push_back(family_column);
		}
		del.__set_columns(family_columns);
		del.__set_durability(delete_flag);
		dels.push_back(del);
	}

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				if(dels.size() > 1)
					(*hbase_client)->deleteMultiple(dels_r, table_name, dels);
				else
					(*hbase_client)->deleteSingle(table_name, dels.front());
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
		        break;
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(m_enable_log, "erase to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
				}

				if(!hbase_client->is_connected())
				{
					if ( !Reconnet(hbase_client) ) break;
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "erase to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "erase to %s exception: %s", table_name.c_str(), ex.what());
				break;
			}
	}

	return false;
}
////////////////////////////////////////////////GET//////////////////////////////////////////////////////数据量和逻辑比较复杂

bool CHbaseClientHelper::Get(const std::string& table_name, TRow& row, TRow::HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version)
{
	std::vector<TRow> multi_row;
	multi_row.push_back(row);
	bool result  = Get(table_name, multi_row, time_range, str_filter, max_version);
	row = multi_row[0];
	return result;
}

bool CHbaseClientHelper::Get(const std::string& table_name, std::vector<TRow>& row_list, TRow::HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version)
{
	if(row_list.empty())
		return false;

	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	std::vector<apache::hadoop::hbase::thrift2::TGet> gets;
	std::vector<apache::hadoop::hbase::thrift2::TResult> result;
	for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
	{
		apache::hadoop::hbase::thrift2::TGet get;
		get.__set_row(iter->get_rowkey());

		if(time_range != NULL)
		{
			if(time_range->first > 0 && time_range->second > 0 )
			{
				apache::hadoop::hbase::thrift2::TTimeRange timerange;
				timerange.__set_minStamp(time_range->first);
				timerange.__set_maxStamp(time_range->second);
				get.__set_timeRange(timerange);
			}
		}

		if(max_version > 0)
			get.__set_maxVersions(max_version);
		if(!str_filter.empty())
			get.__set_filterString(str_filter);

		std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
		for(std::vector<TCell>::const_iterator iter_ = iter->get_cells().begin(); iter_ != iter->get_cells().end(); iter_++)
		{
			    apache::hadoop::hbase::thrift2::TColumn  family_column;
			    family_column.__set_family(iter_->m_Family);
			    family_column.__set_qualifier(iter_->m_Qualifier);
			    family_columns.push_back(family_column);
		}
		get.__set_columns(family_columns);
		gets.push_back(get);
	}

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				if(gets.size() > 1)
					(*hbase_client)->getMultiple(result, table_name, gets);
				else
				{
					apache::hadoop::hbase::thrift2::TResult get_r;
					(*hbase_client)->get(get_r, table_name, gets.front());
					result.push_back(get_r);
				}

				row_list.clear();
				for(std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator iter = result.begin(); iter != result.end(); iter ++)
				{
					const std::string& row_key = iter->row;
					TRow row;
					row.set_rowkey(row_key);
					for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter_ = iter->columnValues.begin(); iter_ != iter->columnValues.end(); iter_++)
					{
						const std::string& family_name  = iter_->family;
						const std::string& column_name  = iter_->qualifier;
						const std::string& column_value = iter_->value;
						uint64_t         column_timestamp = iter_->timestamp;
						row.add_value(family_name, column_name, column_value, column_timestamp);
					}
					row_list.push_back(row);
				}

				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
		        break;
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(m_enable_log, "get from to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
				}

				if(!hbase_client->is_connected())
				{
					if ( !Reconnet(hbase_client) ) break;
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "get from %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "get from %s exception: %s", table_name.c_str(), ex.what());
				break;
			}
	}

	return false;
}

bool CHbaseClientHelper::Get(const std::string& table_name, const std::string& begin_row, const std::string& stop_row, std::vector<TRow>& row_list, uint16_t num_rows, TRow::HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version)
{
	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	apache::hadoop::hbase::thrift2::TScan scan;
	scan.__set_startRow(begin_row);
	scan.__set_stopRow(stop_row);

	if(!str_filter.empty() )
		scan.__set_filterString(str_filter);

	if(time_range != NULL)
	{
		if(time_range->first > 0 && time_range->second > 0 )
		{
			apache::hadoop::hbase::thrift2::TTimeRange timerange;
			timerange.__set_minStamp(time_range->first);
			timerange.__set_maxStamp(time_range->second);
			scan.__set_timeRange(timerange);
		}
	}

	if(max_version > 0)
		scan.__set_maxVersions(max_version);

	std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
	for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter++)
	{
		for(std::vector<TCell>::const_iterator iter_ = iter->get_cells().begin(); iter_!= iter->get_cells().end(); iter_++)
		{
		    apache::hadoop::hbase::thrift2::TColumn  family_column;
		    family_column.__set_family(iter_->m_Family);
		    family_column.__set_qualifier(iter_->m_Qualifier);
		    family_columns.push_back(family_column);
		}
	}
	scan.__set_columns(family_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
		try
		{
			std::vector<apache::hadoop::hbase::thrift2::TResult> results;
			(*hbase_client)->getScannerResults(results, table_name, scan, num_rows);   //number of size 是cell的size，不是row的size

			row_list.clear();
			for(std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator iter = results.begin(); iter != results.end(); iter++)
			{
				const std::string& row_key = iter->row;
				TRow row;
				row.set_rowkey(row_key);
				for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter_ = iter->columnValues.begin(); iter_ != iter->columnValues.end(); iter_++)
				{
					const std::string& family_name = iter_->family;
					const std::string& column_name = iter_->qualifier;
					const std::string& column_value= iter_->value;
					uint64_t        column_timestamp = iter_->timestamp;
					row.add_value(family_name, column_name, column_value, column_timestamp);
				}
				row_list.push_back(row);
			}
			return true;
		}

	    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
	    {
	        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
	        break;
	    }
		catch (apache::thrift::transport::TTransportException& ex)
		{
			if ( retry_times - 1 == i)
			{
				__HLOG_ERROR(m_enable_log, "get from %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			}

			if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
			{
				hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
			}

			if(!hbase_client->is_connected())
			{
				if ( !Reconnet(hbase_client) ) break;
			}
		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(m_enable_log, "get from %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			break;
		}
		catch (apache::thrift::TException& ex)
		{
			__HLOG_ERROR(m_enable_log, "get from %s exception: %s", table_name.c_str(), ex.what());
			break;
		}
	}
	return false;
}

/////////////////////////////////////////////////////////////UPDATA////////////////////////////////////////////////
bool CHbaseClientHelper::Append(const std::string& table_name, const TRow& row, TDurability::type append_flag)
{
	if(row.m_Row_Key.empty())
		return false;

	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	//根据类型来判断 request 的类型
	apache::hadoop::hbase::thrift2::TAppend   append;
	std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_append_columns;

	append.__set_durability(append_flag);
	append.__set_row(row.get_rowkey());
	for(std::vector<TCell>::const_iterator iter = row.get_cells().begin(); iter != row.get_cells().end(); iter++)
	{
		apache::hadoop::hbase::thrift2::TColumnValue  family_column;
		family_column.__set_family(iter->m_Family);
		family_column.__set_qualifier(iter->m_Qualifier);
		family_column.__set_value(iter->m_Value);
		family_append_columns.push_back(family_column);
	}
	append.__set_columns(family_append_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				apache::hadoop::hbase::thrift2::TResult request;
				(*hbase_client)->append(request ,table_name, append);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
		        break;
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(m_enable_log, "append to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
				}

				if(!hbase_client->is_connected())
				{
					if ( !Reconnet(hbase_client) ) break;
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "append to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "append to %s exception: %s", table_name.c_str(), ex.what());
				break;
			}
	}

	return false;
}

bool CHbaseClientHelper::Increment(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, int64_t column_value, TDurability::type increment_flag)
{
	TRow row;
	std::vector<int64_t> result;
	row.set_rowkey(row_key);
	row.add_value(family_name, column_name, column_value);
	return Increment(table_name, row, increment_flag);
}


bool CHbaseClientHelper::Increment(const std::string& table_name, const TRow& row, TDurability::type increment_flag)
{
	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());

	apache::hadoop::hbase::thrift2::TIncrement increment;
	std::vector<apache::hadoop::hbase::thrift2::TColumnIncrement> family_increment_columns;
	std::vector<int64_t> responses;

	increment.__set_durability(increment_flag);
	increment.__set_row(row.get_rowkey());
	for(std::vector<TCell>::const_iterator iter = row.get_cells().begin(); iter != row.get_cells().end(); iter++)
	{
#if __WORDSIZE == 64
		int64_t inc64 =	atoll(iter->m_Value.c_str());
#else
		int64_t inc64 =	atol(iter->m_Value.c_str());
#endif
	    apache::hadoop::hbase::thrift2::TColumnIncrement  family_column;
	    family_column.__set_family(iter->m_Family);
	    family_column.__set_qualifier(iter->m_Qualifier);
	    family_column.__set_amount(inc64);
	    family_increment_columns.push_back(family_column);
	}
	increment.__set_columns(family_increment_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				apache::hadoop::hbase::thrift2::TResult result;
				(*hbase_client)->increment(result ,table_name, increment);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
		        break;
		    }

			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(m_enable_log, "increment to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
				}

				if(!hbase_client->is_connected())
				{
					if ( !Reconnet(hbase_client) ) break;
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "increment to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "increment to %s exception: %s", table_name.c_str(), ex.what());
				break;
			}
	}

	return false;
}

bool  CHbaseClientHelper::Check_With_Replace(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, const std::string& new_column_value, TDurability::type check_flag)
{
	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	apache::hadoop::hbase::thrift2::TPut put;
	put.__set_row(row_key);
	std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_columns;
	apache::hadoop::hbase::thrift2::TColumnValue  family_column;
	family_column.__set_family(family_name);
	family_column.__set_qualifier(column_name);
	family_column.__set_value(new_column_value);
	family_columns.push_back(family_column);
	put.__set_columnValues(family_columns);
	put.__set_durability(check_flag);
	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				(*hbase_client)->checkAndPut(table_name, row_key, family_name, column_name, old_column_value, put);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
		        break;
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(m_enable_log, "check with replace %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
				}

				if(!hbase_client->is_connected())
				{
					if ( !Reconnet(hbase_client) ) break;
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "check with replace %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "check with replace %s exception: %s", table_name.c_str(), ex.what());
				break;
			}
	}
	return false;
}

bool  CHbaseClientHelper::Check_With_Erase(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, TDurability::type check_flag)
{
	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
	apache::hadoop::hbase::thrift2::TDelete del;
	del.__set_row(row_key);
	std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
    apache::hadoop::hbase::thrift2::TColumn  family_column;
    family_column.__set_family(family_name);
    family_column.__set_qualifier(column_name);
    family_columns.push_back(family_column);
    del.__set_columns(family_columns);
    del.__set_durability(check_flag);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				(*hbase_client)->checkAndDelete(table_name, row_key, family_name, column_name ,old_column_value, del);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(m_enable_log, "IOError: %s\n", ex.what());
		        break;
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(m_enable_log, "check with erase %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					hbase_client = static_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* >(Get_Random_Service());
				}

				if(!hbase_client->is_connected())
				{
					if ( !Reconnet(hbase_client) ) break;
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(m_enable_log, "check with erase %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				break;
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(m_enable_log, "check with erase %s exception: %s", table_name.c_str(), ex.what());
				break;
			}
	}
	return false;
}
