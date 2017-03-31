/*
 * hbase_client_helper.cpp
 *
 *  Created on: 2016年11月19日
 *      Author: shuntan
 */
#include "hbase_helper.h"

#ifdef __GNUC__
#  define UNUSED(x) UNUSED_ ## x __attribute__((__unused__))
#else
#  define UNUSED(x) UNUSED_ ## x
#endif

//异常捕获类
#define THROW_HBASE_EXCEPTION(errcode, errmsg) throw CHbaseException(errcode, errmsg, __FILE__, __LINE__)
#define THROW_HBASE_EXCEPTION_WITH_COMMAND(errcode, errmsg, command, key) throw CHbaseException(errcode, errmsg, __FILE__, __LINE__, std::string(), 0, command, key)
#define THROW_HBASE_EXCEPTION_WITH_NODE(errcode, errmsg, node_ip, node_port) throw CHbaseException(errcode, errmsg, __FILE__, __LINE__, node_ip, node_port)
#define THROW_HBASE_EXCEPTION_WITH_NODE_AND_COMMAND(errcode, errmsg, node_ip, node_port, command, key) throw CHbaseException(errcode, errmsg, __FILE__, __LINE__, node_ip, node_port, command, key)

namespace hbase{

bool CHbaseClientHelper::ms_enable_log = true;

static std::string format_string(const char* format, ...)
{
    va_list ap;
    size_t size = getpagesize();
    char* buffer = new char[size];

    while (true)
    {
        va_start(ap, format);
        int expected = vsnprintf(buffer, size, format, ap);

        va_end(ap);
        if (expected > -1 && expected < (int)size)
            break;

        /* Else try again with more space. */
        if (expected > -1)    /* glibc 2.1 */
            size = (size_t)expected + 1; /* precisely what is needed */
        else           /* glibc 2.0 */
            size *= 2;  /* twice the old size */

        delete []buffer;
        buffer = new char[size];
    }

    std::string str = buffer;
    delete []buffer;
    return str;
}

CHbaseException::CHbaseException(int errcode, const std::string& errmsg, const char* file, int line, const std::string& node_ip, int16_t node_port, const char* command, const char* key) throw ()
    : m_errcode(errcode), m_errmsg(errmsg), m_file(file), m_line(line), m_node_ip(node_ip), m_node_port(node_port)
{
    if (command != NULL)
        m_command = command;
    else
        m_command = "#";

    if (key != NULL)
        m_key = key;
    else
        m_key = "#";
}

const char* CHbaseException::what() const throw()
{
    return m_errmsg.c_str();
}

std::string CHbaseException::str() const throw ()
{
    if(m_node_port > 0 && m_node_ip.size() > 8)
        return format_string("HBASE://%s:%d/%s/%s/(%d):%s@%s:%d", m_node_ip.c_str(), m_node_port, m_command.c_str(), m_key.c_str(), m_errcode, m_errmsg.c_str(), m_file.c_str(), m_line);
    else
        return format_string("HBASE://%s/%s/(%d):%s@%s:%d", m_command.c_str(), m_key.c_str(), m_errcode, m_errmsg.c_str(), m_file.c_str(), m_line);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

CHbaseClientHelper::CHbaseClientHelper(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out) throw (CHbaseException) : m_hbase_client(NULL)
{
	std::vector<std::string> host_array;
	split(&host_array, host_list, ",");
	for(std::vector<std::string>::const_iterator iter = host_array.begin(); iter != host_array.end(); iter++)
	{
		std::vector<std::string> ip_port;
		split(&ip_port, *iter, ":");
		if(ip_port.size() != 2)
			continue;

		const std::string& host_ip   = ip_port[0];
		const std::string& host_port = ip_port[1];

		m_hbase_clients.push_back(boost::make_shared<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient> >(host_ip, atoi(host_port.c_str()), connect_timeout, recive_timeout, send_time_out));
	}

	if(m_hbase_clients.size() < 1)
	{
		__HLOG_ERROR(ms_enable_log, "Hbase service hosts empty! \n");
		THROW_HBASE_EXCEPTION(HOST_ERROR, "Hbase service hosts empty");
	}
	else
	{
		connect();
	}
}

CHbaseClientHelper& CHbaseClientHelper::get_singleton(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out) throw (CHbaseException)
{
    return *get_singleton_ptr(host_list, connect_timeout, recive_timeout, send_time_out);
}

CHbaseClientHelper* CHbaseClientHelper::get_singleton_ptr(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out) throw (CHbaseException)
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
}

void* CHbaseClientHelper::get_random_service()
{
    static unsigned int factor = 0;
    std::vector<boost::shared_ptr<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient> > >::size_type i = get_random_number(factor++, static_cast<unsigned int>(m_hbase_clients.size()));
    return m_hbase_clients[i].get();
}

bool CHbaseClientHelper::connect() throw (CHbaseException)
{
    size_t retry_times = 3;
    m_hbase_client =  (CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* )(get_random_service());
    while(retry_times)
    {
        try
        {
            m_hbase_client->connect();
            __HLOG_INFO(ms_enable_log, "connect to node[%s:%u] success\n", m_hbase_client->get_host().c_str(), m_hbase_client->get_port());
            return true;
        }
        catch (apache::thrift::transport::TTransportException& ex)
        {
            __HLOG_INFO(ms_enable_log, "surplus try time [%u] connect hbase://%s:%u, transport(I/O) exception: (%d)(%s)",retry_times, m_hbase_client->get_host().c_str(), m_hbase_client->get_port(), ex.getType(), ex.what());
            (retry_times > 1)? THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_client->get_host(), m_hbase_client->get_port()) : retry_times --;
            usleep(100);
        }
        catch (apache::thrift::TApplicationException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "connect hbase://%s:%u application exception: %s",m_hbase_client->get_host().c_str(), m_hbase_client->get_port(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_client->get_host(), m_hbase_client->get_port());
        }
        catch (apache::thrift::TException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "connect hbase://%s:%u,exception: [%s].", m_hbase_client->get_host().c_str(), m_hbase_client->get_port(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_NODE(CONNECT_FAILED, ex.what(), m_hbase_client->get_host(), m_hbase_client->get_port());
        }
    }

    return false;
}

bool CHbaseClientHelper::reconnect(bool random) throw (CHbaseException)
{
    if(random)
        m_hbase_client =  (CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* )(get_random_service());

	try
	{
	    m_hbase_client->connect();
		return true;
	}
	catch (apache::thrift::transport::TTransportException& ex)
	{
	    if( apache::thrift::transport::TTransportException::TIMED_OUT != ex.getType())
	    	THROW_HBASE_EXCEPTION_WITH_NODE_AND_COMMAND(ex.getType(), ex.what(), m_hbase_client->get_host(), m_hbase_client->get_port(), "reconnect", NULL);
	}

    return false;
}

bool CHbaseClientHelper::exist(const std::string& table_name,  const TRow& row) throw (CHbaseException)
{
	apache::hadoop::hbase::thrift2::TGet get;

	get.__set_row(row.get_rowkey());
	std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
	for(std::vector<TCell>::const_iterator iter = row.get_columns().begin(); iter != row.get_columns().end(); iter++)
	{
	    apache::hadoop::hbase::thrift2::TColumn  family_column;
	    family_column.__set_family(iter->m_family);
	    if(!iter->m_qualifier.empty())
	        family_column.__set_qualifier(iter->m_qualifier);
	    family_columns.push_back(family_column);
	}
	get.__set_columns(family_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
		try
		{
			bool bResult;
			bResult = (*m_hbase_client)->exists(table_name, get);
			return bResult;
		}

	    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
	    {
	        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
	        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HEXISTS", table_name.c_str());
	    }

		catch (apache::thrift::transport::TTransportException& ex)
		{
			// type = 2 或者 unknow的时候才需要重连
			if ( retry_times - 1 == i)
			{
				__HLOG_ERROR(ms_enable_log, "exists %s transport exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HEXISTS", table_name.c_str());
			}

			if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
			{
			    reconnect();
			}
		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "exists %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HEXISTS", table_name.c_str());
		}
		catch (apache::thrift::TException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "exists %s exception: %s", table_name.c_str(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_EXIST, ex.what(), "HEXISTS", table_name.c_str());
		}
	}

	return false;
}

/////////////////////////////////////////////////////INSERT////////////////////////////////////////////////////////////////////

bool CHbaseClientHelper::put(const std::string& table_name, const TRow& row, TDurability::type insert_flag, uint64_t time_stamp) throw (CHbaseException)
{
	std::vector<TRow> multi_row;
	multi_row.push_back(row);
	return put_multi(table_name, multi_row, insert_flag, time_stamp);
}

bool CHbaseClientHelper::put_multi(const std::string& table_name, const std::vector<TRow>& row_list, TDurability::type insert_flag, uint64_t time_stamp) throw (CHbaseException)
{
	if(row_list.empty())
		return false;

	std::vector<apache::hadoop::hbase::thrift2::TPut> put_vec;
	for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
	{
	    if(iter->m_option != PUT)
	        THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");

		apache::hadoop::hbase::thrift2::TPut put;
		put.__set_row(iter->get_rowkey());
		if(time_stamp > 0)
		{
			put.__set_timestamp(time_stamp);
		}
		std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_columns;
		for(std::vector<TCell>::const_iterator iter_ = iter->get_columns().begin(); iter_ != iter->get_columns().end(); iter_++)
		{
		    apache::hadoop::hbase::thrift2::TColumnValue  family_column;
		    family_column.__set_family(iter_->m_family);
		    family_column.__set_qualifier(iter_->m_qualifier);
		    family_column.__set_value(iter_->m_value);
		    family_columns.push_back(family_column);
		}
		put.__set_columnValues(family_columns);
		put.__set_durability(insert_flag);
		put_vec.push_back(put);
	}

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				if(put_vec.size() > 1)
					(*m_hbase_client)->putMultiple(table_name, put_vec);
				else
					(*m_hbase_client)->put(table_name, put_vec.front());
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HPUT", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log,	 "put to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HPUT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
				    reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "put to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HPUT", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "put to %s exception: %s", table_name.c_str(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_PUT, ex.what(), "HPUT", table_name.c_str());
			}
	}

	return false;
}

///////////////////////////////////////////////////////ERASE///////////////////////////////////////////////////////////

bool CHbaseClientHelper::erase(const std::string& table_name, const TRow& row, TDurability::type delete_flag, uint64_t time_stamp) throw (CHbaseException)
{
	std::vector<TRow> multi_row;
	multi_row.push_back(row);
	return erase_multi(table_name, multi_row, delete_flag, time_stamp);
}

bool CHbaseClientHelper::erase_multi(const std::string& table_name, const std::vector<TRow>& row_list, TDurability::type delete_flag, uint64_t time_stamp) throw (CHbaseException)
{
	if(row_list.empty())
		return false;

	std::vector<apache::hadoop::hbase::thrift2::TDelete> del_vec;
	std::vector<apache::hadoop::hbase::thrift2::TDelete> del_vec_r;
	for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
	{
        if(iter->m_option != DELETE)
            THROW_HBASE_EXCEPTION(HBASE_DELETE, "OPTION NOT RIGHT");

		apache::hadoop::hbase::thrift2::TDelete del;
		del.__set_row(iter->get_rowkey());
		if(time_stamp > 0)
		{
			del.__set_timestamp(time_stamp);
		}

		std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
		for(std::vector<TCell>::const_iterator iter_ = iter->get_columns().begin(); iter_ != iter->get_columns().end(); iter_++)
		{
            apache::hadoop::hbase::thrift2::TColumn  family_column;
            family_column.__set_family(iter_->m_family);
            if(!iter_->m_qualifier.empty())
                family_column.__set_qualifier(iter_->m_qualifier);
            family_columns.push_back(family_column);
		}

		del.__set_deleteType((family_columns.size() > 1)? TDeleteType::DELETE_COLUMNS : TDeleteType::DELETE_COLUMN);
		del.__set_columns(family_columns);
		del.__set_durability(delete_flag);
		del_vec.push_back(del);
	}

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				if(del_vec.size() > 1)
					(*m_hbase_client)->deleteMultiple(del_vec_r, table_name, del_vec);
				else
					(*m_hbase_client)->deleteSingle(table_name, del_vec.front());
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HDELETE", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "erase to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HDELETE", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "erase to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HDELETE", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "erase to %s exception: %s", table_name.c_str(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_DELETE, ex.what(), "HDELETE", table_name.c_str());
			}
	}

	return false;
}
////////////////////////////////////////////////GET//////////////////////////////////////////////////////数据量和逻辑比较复杂

bool CHbaseClientHelper::get(const std::string& table_name, TRow& row, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version) throw (CHbaseException)
{
	std::vector<TRow> multi_row;
	multi_row.push_back(row);
	bool result  = get_multi(table_name, multi_row, time_range, str_filter, max_version);
	row = multi_row[0];
	return result;
}

bool CHbaseClientHelper::get_multi(const std::string& table_name, std::vector<TRow>& row_list, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version) throw (CHbaseException)
{
	if(row_list.empty())
		return false;

	std::vector<apache::hadoop::hbase::thrift2::TGet> get_vec;
	std::vector<apache::hadoop::hbase::thrift2::TResult> result;
	for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
	{
        if(iter->m_option != GET)
            THROW_HBASE_EXCEPTION(HBASE_GET, "OPTION NOT RIGHT");

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
		for(std::vector<TCell>::const_iterator iter_ = iter->get_columns().begin(); iter_ != iter->get_columns().end(); iter_++)
		{
			    apache::hadoop::hbase::thrift2::TColumn  family_column;
			    family_column.__set_family(iter_->m_family);
			    if(!iter_->m_qualifier.empty())
			        family_column.__set_qualifier(iter_->m_qualifier);
			    family_columns.push_back(family_column);
		}
		get.__set_columns(family_columns);
		get_vec.push_back(get);
	}

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				if(get_vec.size() > 1)
					(*m_hbase_client)->getMultiple(result, table_name, get_vec);
				else
				{
					apache::hadoop::hbase::thrift2::TResult get_r;
					(*m_hbase_client)->get(get_r, table_name, get_vec.front());
					result.push_back(get_r);
				}

				row_list.clear();
				for(std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator iter = result.begin(); iter != result.end(); iter ++)
				{
					const std::string& row_key = iter->row;
					TRow row(GET);
					row.set_rowkey(row_key);
					for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter_ = iter->columnValues.begin(); iter_ != iter->columnValues.end(); iter_++)
					{
						const std::string& family_name  = iter_->family;
						const std::string& column_name  = iter_->qualifier;
						const std::string& column_value = iter_->value;
						uint64_t       column_timestamp = iter_->timestamp;
						row.add_column_value(family_name, column_name, column_value, column_timestamp);
					}
					row_list.push_back(row);
				}

				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HGET", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "get from to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGET", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGET", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s exception: %s", table_name.c_str(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_GET, ex.what(), "HGET", table_name.c_str());
			}
	}

	return false;
}

bool CHbaseClientHelper::get_by_scan(const std::string& table_name, const std::string& begin_row, const std::string& stop_row, std::vector<TRow>& row_list, uint16_t num_rows, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version) throw (CHbaseException)
{
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
		for(std::vector<TCell>::const_iterator iter_ = iter->get_columns().begin(); iter_!= iter->get_columns().end(); iter_++)
		{
		    apache::hadoop::hbase::thrift2::TColumn  family_column;
		    family_column.__set_family(iter_->m_family);
		    if(!iter_->m_qualifier.empty())
		        family_column.__set_qualifier(iter_->m_qualifier);
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
			(*m_hbase_client)->getScannerResults(results, table_name, scan, num_rows);   //number of size 是cell的size，不是row的size

			row_list.clear();
			for(std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator iter = results.begin(); iter != results.end(); iter++)
			{
				const std::string& row_key = iter->row;
				TRow row(GET);
				row.set_rowkey(row_key);
				for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter_ = iter->columnValues.begin(); iter_ != iter->columnValues.end(); iter_++)
				{
					const std::string& family_name = iter_->family;
					const std::string& column_name = iter_->qualifier;
					const std::string& column_value = iter_->value;
					uint64_t column_timestamp = iter_->timestamp;
					row.add_column_value(family_name, column_name, column_value, column_timestamp);
				}
				row_list.push_back(row);
			}
			return true;
		}

	    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
	    {
	        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
	        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HGETSCAN", table_name.c_str());
	    }
		catch (apache::thrift::transport::TTransportException& ex)
		{
			if ( retry_times - 1 == i)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGETSCAN", table_name.c_str());
			}

			if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
			{
				reconnect();
			}

		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "scan get from %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGETSCAN", table_name.c_str());
		}
		catch (apache::thrift::TException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "scan get from %s exception: %s", table_name.c_str(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_SCAN_GET, ex.what(), "HGETSCAN", table_name.c_str());
		}
	}
	return false;
}

/////////////////////////////////////////////////////////////UPDATA////////////////////////////////////////////////
bool CHbaseClientHelper::append(const std::string& table_name, const TRow& row, TDurability::type append_flag) throw (CHbaseException)
{
	if(row.m_row_key.empty())
		return false;

    if(row.m_option != PUT)
        THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");

	//根据类型来判断 request 的类型
	apache::hadoop::hbase::thrift2::TAppend   append;
	std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_append_columns;

	append.__set_durability(append_flag);
	append.__set_row(row.get_rowkey());
	for(std::vector<TCell>::const_iterator iter = row.get_columns().begin(); iter != row.get_columns().end(); iter++)
	{
		apache::hadoop::hbase::thrift2::TColumnValue  family_column;
		family_column.__set_family(iter->m_family);
		family_column.__set_qualifier(iter->m_qualifier);
		family_column.__set_value(iter->m_value);
		family_append_columns.push_back(family_column);
	}
	append.__set_columns(family_append_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				apache::hadoop::hbase::thrift2::TResult request;
				(*m_hbase_client)->append(request ,table_name, append);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HAPPEND", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "append to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HAPPEND", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "append to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HAPPEND", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "append to %s exception: %s", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_APPEND, ex.what(), "HAPPEND", table_name.c_str());
			}
	}

	return false;
}

bool CHbaseClientHelper::increment(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, int64_t column_value, TDurability::type increment_flag) throw (CHbaseException)
{
	TRow row(hbase::PUT);
	std::vector<int64_t> result;
	row.set_rowkey(row_key);
	row.add_column_value(family_name, column_name, column_value);
	return increment_multi(table_name, row, increment_flag);
}


bool CHbaseClientHelper::increment_multi(const std::string& table_name, const TRow& row, TDurability::type increment_flag) throw (CHbaseException)
{
    if(row.m_option != PUT)
        THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");

	apache::hadoop::hbase::thrift2::TIncrement increment;
	std::vector<apache::hadoop::hbase::thrift2::TColumnIncrement> family_increment_columns;
	std::vector<int64_t> responses;

	increment.__set_durability(increment_flag);
	increment.__set_row(row.get_rowkey());
	for(std::vector<TCell>::const_iterator iter = row.get_columns().begin(); iter != row.get_columns().end(); iter++)
	{
#if __WORDSIZE == 64
		int64_t inc64 =	atoll(iter->m_Value.c_str());
#else
		int64_t inc64 =	atol(iter->m_value.c_str());
#endif
	    apache::hadoop::hbase::thrift2::TColumnIncrement  family_column;
	    family_column.__set_family(iter->m_family);
	    family_column.__set_qualifier(iter->m_qualifier);
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
				(*m_hbase_client)->increment(result ,table_name, increment);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HINCREMENT", table_name.c_str());
		    }

			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "increment to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HINCREMENT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "increment to %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HINCREMENT", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "increment to %s exception: %s", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_INCREMENT, ex.what(), "HINCREMENT", table_name.c_str());
			}
	}

	return false;
}

bool  CHbaseClientHelper::check_and_put(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, const std::string& new_column_value, TDurability::type check_flag) throw (CHbaseException)
{
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
				(*m_hbase_client)->checkAndPut(table_name, row_key, family_name, column_name, old_column_value, put);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "check with replace %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with replace %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with replace %s exception: %s", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_CHECK_PUT, ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
			}
	}
	return false;
}

bool  CHbaseClientHelper::check_and_erase(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, TDurability::type check_flag) throw (CHbaseException)
{
	apache::hadoop::hbase::thrift2::TDelete del;
	del.__set_row(row_key);
	std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
    apache::hadoop::hbase::thrift2::TColumn  family_column;
    family_column.__set_family(family_name);
    if(!column_name.empty())
        family_column.__set_qualifier(column_name);
    family_columns.push_back(family_column);
    del.__set_columns(family_columns);
    del.__set_durability(check_flag);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				(*m_hbase_client)->checkAndDelete(table_name, row_key, family_name, column_name ,old_column_value, del);
				return true;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( retry_times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "check with erase %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
				    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with erase %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with erase %s exception: %s", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HABSE_CHECK_DELEET, ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
			}
	}
	return false;
}

bool  CHbaseClientHelper::combination(const std::string& table_name, const std::vector<TRow>& row_list, TDurability::type options_flag, int64_t time_stamp) throw (CHbaseException)
{
    if(row_list.empty())
        return false;

    std::string row_key_sameone;
    TRowMutations combs;
    std::vector<apache::hadoop::hbase::thrift2::TMutation> comb_vec;
    std::vector<apache::hadoop::hbase::thrift2::TResult> result;
    for(std::vector<TRow>::const_iterator iter = row_list.begin(); iter != row_list.end(); iter ++)
    {
        apache::hadoop::hbase::thrift2::TMutation comb;
        apache::hadoop::hbase::thrift2::TPut put;
        apache::hadoop::hbase::thrift2::TDelete del;
        if(!row_key_sameone.empty() && row_key_sameone != iter->get_rowkey())
            THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "NO MORE ONE ROWKEY");
        else
            row_key_sameone = iter->get_rowkey();

        if(iter->m_option == PUT)
        {
            put.__set_row(row_key_sameone);

            if(time_stamp > 0)
            {
                put.__set_timestamp(time_stamp);
            }

            std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_columns;
            for(std::vector<TCell>::const_iterator iter_ = iter->get_columns().begin(); iter_ != iter->get_columns().end(); iter_++)
            {
                apache::hadoop::hbase::thrift2::TColumnValue  family_column;
                family_column.__set_family(iter_->m_family);
                family_column.__set_qualifier(iter_->m_qualifier);
                family_column.__set_value(iter_->m_value);
                family_columns.push_back(family_column);
            }

            put.__set_durability(options_flag);
            put.__set_columnValues(family_columns);
            comb.__set_put(put);
        }

        if(iter->m_option == DELETE)
        {
            del.__set_row(row_key_sameone);

            if(time_stamp > 0)
            {
                del.__set_timestamp(time_stamp);
            }

            if(iter->get_columns().size() > 1)
                THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "NOT DELETE COLUMN SINGLE");

            std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;;
            const TCell& iter_ = iter->get_columns().front();
            apache::hadoop::hbase::thrift2::TColumn  family_column;
            family_column.__set_family(iter_.m_family);
            family_column.__set_qualifier(iter_.m_qualifier);
            family_columns.push_back(family_column);

            del.__set_deleteType(TDeleteType::DELETE_COLUMN);
            del.__set_columns(family_columns);
            del.__set_durability(options_flag);
            comb.__set_deleteSingle(del);
        }

        else
        {
            THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "OPTION NOT RIGHT");
        }

        comb_vec.push_back(comb);
    }
    combs.__set_mutations(comb_vec);
    combs.__set_row(row_key_sameone);

    const int retry_times = 2;
    for (int i=0; i<retry_times; ++i)
    {
            try
            {
                (*m_hbase_client)->mutateRow(table_name, combs);
                return true;
            }

            catch (apache::hadoop::hbase::thrift2::TIOError& ex)
            {
                __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.what(), "HCOMBINATION,", table_name.c_str());
            }
            catch (apache::thrift::transport::TTransportException& ex)
            {
                if ( retry_times - 1 == i)
                {
                    __HLOG_ERROR(ms_enable_log, "comb from to %s transport exception(I/0): (%d)%s", table_name.c_str(), ex.getType(), ex.what());
                    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCOMBINATION,", table_name.c_str());
                }

                if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
                {
                    reconnect();
                }
            }
            catch (apache::thrift::TApplicationException& ex)
            {
                __HLOG_ERROR(ms_enable_log, "comb from %s application exception: (%d)%s", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCOMBINATION,", table_name.c_str());
            }
            catch (apache::thrift::TException& ex)
            {
                __HLOG_ERROR(ms_enable_log, "comb from %s exception: %s", table_name.c_str(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_COMBINATION, ex.what(), "HCOMBINATION,", table_name.c_str());
            }
    }

    return false;
}

}
