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

#define __HLOG_ERROR(status, format, ...) \
{  \
    if(status)  \
    {   \
        printf("[HB-ERROR][%s:%d]", __FILE__, __LINE__); \
        printf(format, ##__VA_ARGS__); \
    }   \
}

#define __HLOG_INFO(status, format, ...) \
{  \
    if(status)  \
    {   \
        printf("[HB-INFO][%s:%d]", __FILE__, __LINE__); \
        printf(format, ##__VA_ARGS__); \
    }   \
}

#define __HLOG_DEBUG(status, format, ...) \
{  \
    if(status)  \
    {   \
        printf("[HB-DEBUG][%s:%d]", __FILE__, __LINE__); \
        printf(format, ##__VA_ARGS__); \
    }   \
}


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

template <class ContainerType>
static int split(ContainerType* tokens, const std::string& source, const std::string& sep, bool skip_sep=false)
{
    if (sep.empty())
    {
        tokens->push_back(source);
    }
    else if (!source.empty())
    {
        std::string str = source;
        std::string::size_type pos = str.find(sep);

        while (true)
        {
            std::string token = str.substr(0, pos);
            tokens->push_back(token);

            if (std::string::npos == pos)
            {
                break;
            }
            if (skip_sep)
            {
                bool end = false;
                while (0 == strncmp(sep.c_str(), &str[pos+1], sep.size()))
                {
                    pos += sep.size();
                    if (pos >= str.size())
                    {
                        end = true;
                        tokens->push_back(std::string(""));
                        break;
                    }
                }

                if (end)
                    break;
            }

            str = str.substr(pos + sep.size());
            pos = str.find(sep);
        }
    }

    return static_cast<int>(tokens->size());
}

// 取随机数
template <typename T>
static T get_random_number(unsigned int i, T max_number)
{
    struct timeval tv;
    struct timezone *tz = NULL;

    gettimeofday(&tv, tz);
    srandom(tv.tv_usec + i); // 加入i，以解决过快时tv_usec值相同

    // RAND_MAX 类似于INT_MAX
    return static_cast<T>(random() % max_number);
}

template <typename T>
static std::string int_tostring(T num)
{
    std::ostringstream stream;
    stream<<num;
    return stream.str();
}

static void thrift_log(const char* log)
{
    __HLOG_INFO(CHbaseClientHelper::ms_enable_log, "%s", log);
}

///////////////////////////////////////CThriftClientHelper/////////////////////////////////////////

template <class ThriftClient>
CThriftClientHelper<ThriftClient>::CThriftClientHelper(
        const std::string &host, uint16_t port,
        int connect_timeout_milliseconds,
        int receive_timeout_milliseconds,
        int send_timeout_milliseconds,
        TProtocolType protocol_type,
        TransportType transport_type): m_host(host), m_port(port)
{
    apache::thrift::GlobalOutput.setOutputFunction(thrift_log);
    m_sock_pool.reset(new apache::thrift::transport::TSocketPool());
    m_sock_pool->addServer(host, (int)port);
    m_sock_pool->setConnTimeout(connect_timeout_milliseconds);
    m_sock_pool->setRecvTimeout(receive_timeout_milliseconds);
    m_sock_pool->setSendTimeout(send_timeout_milliseconds);
    m_socket = m_sock_pool;

    switch(transport_type)
    {
        case BUFFERED:
            m_transport.reset(new apache::thrift::transport::TBufferedTransport(m_socket));
            break;

        case FRAMED:
            m_transport.reset(new apache::thrift::transport::TFramedTransport(m_socket));
            break;

        //case FILE:
        //    _transport.reset(new apache::thrift::transport::TFileTransport(_socket.get()->));
        //    break;
    }

    switch(protocol_type)
    {
        case BINARY:
            m_protocol.reset(new apache::thrift::protocol::TBinaryProtocol(m_transport));
            break;

        case COMPACT:
            m_protocol.reset(new apache::thrift::protocol::TCompactProtocol(m_transport));
            break;

        case JSON:
            m_protocol.reset(new apache::thrift::protocol::TJSONProtocol(m_transport));
            break;

        case DENSE:
            m_protocol.reset(new apache::thrift::protocol::TDenseProtocol(m_transport));
            break;

        case DEBUG:
            m_protocol.reset(new apache::thrift::protocol::TDebugProtocol(m_transport));
            break;
    }

    m_client.reset(new ThriftClient(m_protocol));
}

template <class ThriftClient>
CThriftClientHelper<ThriftClient>::~CThriftClientHelper()
{
    close();
}

template <class ThriftClient>
void CThriftClientHelper<ThriftClient>::connect()
{
    if (!m_transport->isOpen())
    {
        m_transport->open();
    }
}

template <class ThriftClient>
bool CThriftClientHelper<ThriftClient>::is_connected() const
{
    return m_transport->isOpen();
}

template <class ThriftClient>
void CThriftClientHelper<ThriftClient>::close()
{
    if (m_transport->isOpen())
    {
        m_transport->close();
    }
}

template <class ThriftClient>
inline ThriftClient* CThriftClientHelper<ThriftClient>::get()
{
    return m_client.get();
}

template <class ThriftClient>
inline ThriftClient* CThriftClientHelper<ThriftClient>::get() const
{
    return m_client.get();
}

template <class ThriftClient>
inline ThriftClient* CThriftClientHelper<ThriftClient>::operator ->()
{
    return get();
}

template <class ThriftClient>
inline ThriftClient* CThriftClientHelper<ThriftClient>::operator ->() const
{
    return get();
}

template <class ThriftClient>
inline const std::string& CThriftClientHelper<ThriftClient>::get_host() const
{
    return m_host;
}

template <class ThriftClient>
inline uint16_t CThriftClientHelper<ThriftClient>::get_port() const
{
    return m_port;
}

template <class ThriftClient>
std::string CThriftClientHelper<ThriftClient>::str() const
{
    char acTmp[64];
    memset(acTmp, 0, sizeof(acTmp));
    sprintf(acTmp,"thrift://%s:%u", m_host.c_str(), m_port);
    return std::string(acTmp);
}

//////////////////////////////CHbaseException/////////////////////////////////

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

inline const char* CHbaseException::what() const throw()
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

////////////////////////////////////////////CHbaseClientHelper///////////////////////////////////////////////////////////

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

    try
    {
        if(!s_singleton)
        {
            s_singleton = new CHbaseClientHelper(host_list, connect_timeout, recive_timeout, send_time_out);
        }
    }
    catch(hbase::CHbaseException& ex)
    {
        return NULL;
    }

    return s_singleton;
}

void CHbaseClientHelper::ignore_log()
{
    ms_enable_log = false;
}

CHbaseClientHelper::~CHbaseClientHelper()
{
    close();
}

void* CHbaseClientHelper::get_random_service()
{
    static unsigned int factor = 0;
    std::vector<boost::shared_ptr<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient> > >::size_type i = get_random_number(factor++, static_cast<unsigned int>(m_hbase_clients.size()));
    return m_hbase_clients[i].get();
}

template <typename T> T CHbaseClientHelper::hbase_shell(const std::string& habse_shell) throw (CHbaseException)
{
    T t;
    // 未实现,误用!!!
    // int argc = 0;
    // char** argv = new char*[argc];  // 保存临时参数的二级指针
    // argc1 为操作command
    // argc2 为表名
    //scan 'tablename',STARTROW=>'start',COLUMNS=>['family:qualifier'],FILTER=>SingleColumnValueFilter.new(Bytes.toBytes('family'),Bytes.toBytes('qualifier'))
    return t;
}

bool CHbaseClientHelper::connect() throw (CHbaseException)
{
    size_t retry_times = 3;
    m_hbase_client =  reinterpret_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>*>(get_random_service());
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
            __HLOG_INFO(ms_enable_log, "surplus try time [%lu] connect hbase://%s:%u, transport(I/O) exception: (%d)(%s)\n",retry_times, m_hbase_client->get_host().c_str(), m_hbase_client->get_port(), ex.getType(), ex.what());
            (retry_times > 1)? THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_client->get_host(), m_hbase_client->get_port()) : retry_times --;
            usleep(100);
        }
        catch (apache::thrift::TApplicationException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "connect hbase://%s:%u application exception: %s\n",m_hbase_client->get_host().c_str(), m_hbase_client->get_port(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_client->get_host(), m_hbase_client->get_port());
        }
        catch (apache::thrift::TException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "connect hbase://%s:%u,exception: [%s].\n", m_hbase_client->get_host().c_str(), m_hbase_client->get_port(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_NODE(CONNECT_FAILED, ex.what(), m_hbase_client->get_host(), m_hbase_client->get_port());
        }
    }

    return false;
}

bool CHbaseClientHelper::reconnect(bool random) throw (CHbaseException)
{
    if(random)
        m_hbase_client =  reinterpret_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>*>(get_random_service());

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

void CHbaseClientHelper::close() throw (CHbaseException)
{
    for(std::vector<boost::shared_ptr<void> >::const_iterator iter = m_hbase_clients.begin(); iter != m_hbase_clients.end(); iter++)
    {
        reinterpret_cast<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>*>(iter->get())->close();
    }
}

void CHbaseClientHelper::update(const char* format, ...) throw (CHbaseException)
{
    int excepted = 0;
    size_t hbase_sql_size = getpagesize();
    boost::scoped_array<char> hbase_sql(new char[hbase_sql_size]);
    va_list ap;

    while (true)
    {
        va_start(ap, format);
        excepted = vsnprintf(hbase_sql.get(), hbase_sql_size, format, ap);
        va_end(ap);

        /* If that worked, return the string. */
        if (excepted > -1 && excepted < (int)hbase_sql_size)
            break;

        /* Else try again with more space. */
        if (excepted > -1)    /* glibc 2.1 */
            hbase_sql_size = (size_t)excepted + 1; /* precisely what is needed */
        else           /* glibc 2.0 */
            hbase_sql_size *= 2;  /* twice the old size */

        hbase_sql.reset(new char[hbase_sql_size]);
    }

    hbase_shell<int>(hbase_sql.get());
}

void CHbaseClientHelper::query(HBTable* rows, const char* format, ...) throw (CHbaseException)
{
    int excepted = 0;
    size_t hbase_sql_size = getpagesize();
    boost::scoped_array<char> hbase_sql(new char[hbase_sql_size]);
    va_list ap;

    while (true)
    {
        va_start(ap, format);
        excepted = vsnprintf(hbase_sql.get(), hbase_sql_size, format, ap);
        va_end(ap);

        /* If that worked, return the string. */
        if (excepted > -1 && excepted < (int)hbase_sql_size)
            break;

        /* Else try again with more space. */
        if (excepted > -1)    /* glibc 2.1 */
            hbase_sql_size = (size_t)excepted + 1; /* precisely what is needed */
        else           /* glibc 2.0 */
            hbase_sql_size *= 2;  /* twice the old size */

        hbase_sql.reset(new char[hbase_sql_size]);
    }

    *rows = hbase_shell<HBTable>(hbase_sql.get());
}

void CHbaseClientHelper::query(HBRow* row, const char* format, ...) throw (CHbaseException)
{
    int excepted = 0;
    size_t hbase_sql_size = getpagesize();
    boost::scoped_array<char> hbase_sql(new char[hbase_sql_size]);
    va_list ap;

    while (true)
    {
        va_start(ap, format);
        excepted = vsnprintf(hbase_sql.get(), hbase_sql_size, format, ap);
        va_end(ap);

        /* If that worked, return the string. */
        if (excepted > -1 && excepted < (int)hbase_sql_size)
            break;

        /* Else try again with more space. */
        if (excepted > -1)    /* glibc 2.1 */
            hbase_sql_size = (size_t)excepted + 1; /* precisely what is needed */
        else           /* glibc 2.0 */
            hbase_sql_size *= 2;  /* twice the old size */

        hbase_sql.reset(new char[hbase_sql_size]);
    }

    *row = hbase_shell<HBRow>(hbase_sql.get());
}

std::string CHbaseClientHelper::query(const char* format, ...) throw (CHbaseException)
{
    int excepted = 0;
    size_t hbase_sql_size = getpagesize();
    boost::scoped_array<char> hbase_sql(new char[hbase_sql_size]);
    va_list ap;

    while (true)
    {
        va_start(ap, format);
        excepted = vsnprintf(hbase_sql.get(), hbase_sql_size, format, ap);
        va_end(ap);

        /* If that worked, return the string. */
        if (excepted > -1 && excepted < (int)hbase_sql_size)
            break;

        /* Else try again with more space. */
        if (excepted > -1)    /* glibc 2.1 */
            hbase_sql_size = (size_t)excepted + 1; /* precisely what is needed */
        else           /* glibc 2.0 */
            hbase_sql_size *= 2;  /* twice the old size */

        hbase_sql.reset(new char[hbase_sql_size]);
    }

    return hbase_shell<std::string>(hbase_sql.get());
}

bool CHbaseClientHelper::exist(const std::string& table_name, const std::string& row_key, const HBRow& row) throw (CHbaseException)
{
	apache::hadoop::hbase::thrift2::TGet get;

	get.__set_row(row_key);
    std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
    for(HBRow::const_iterator iter = row.begin(); iter != row.end(); iter++)
    {
        std::vector<std::string> tokener;
        int ret = split(&tokener, iter->first, ":");
        apache::hadoop::hbase::thrift2::TColumn  family_column;
        family_column.__set_family(tokener[0]);
        if(ret>1)
            family_column.__set_qualifier(tokener[1]);
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
				__HLOG_ERROR(ms_enable_log, "exists %s transport exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
				THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HEXISTS", table_name.c_str());
			}

			if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
			{
			    reconnect();
			}
		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "exists %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HEXISTS", table_name.c_str());
		}
		catch (apache::thrift::TException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "exists %s exception: %s\n", table_name.c_str(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_EXIST, ex.what(), "HEXISTS", table_name.c_str());
		}
	}

	return false;
}

/////////////////////////////////////////////////////INSERT////////////////////////////////////////////////////////////////////

void CHbaseClientHelper::put(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type insert_flag, uint64_t time_stamp) throw (CHbaseException)
{
    HBTable multi_row;
	multi_row.insert(std::make_pair(row_key,row));
	put_multi(table_name, multi_row, insert_flag, time_stamp);
}

void CHbaseClientHelper::put_multi(const std::string& table_name, const HBTable& rows, TDurability::type insert_flag, uint64_t time_stamp) throw (CHbaseException)
{
	if(rows.empty())
		return ;

	std::vector<apache::hadoop::hbase::thrift2::TPut> put_vec;
	for(HBTable::const_iterator iter = rows.begin(); iter != rows.end(); iter ++)
	{
	    //if(iter->m_option != PUT)
	    //    THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");

		apache::hadoop::hbase::thrift2::TPut put;
		put.__set_row(iter->first);
		if(time_stamp > 0)
		{
			put.__set_timestamp(time_stamp);
		}

		std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_columns;
		for(HBRow::const_iterator iter_ = iter->second.begin(); iter_ != iter->second.end(); iter_++)
		{
	        std::vector<std::string> tokener;
	        int ret = split(&tokener, iter_->first, ":");
		    apache::hadoop::hbase::thrift2::TColumnValue  family_column;
		    family_column.__set_family(tokener[0]);
		    if(ret>1)
		        family_column.__set_qualifier(tokener[1]);
		    family_column.__set_value(iter_->second.m_value);
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
					__HLOG_ERROR(ms_enable_log,	 "put to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HPUT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
				    reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "put to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HPUT", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "put to %s exception: %s\n", table_name.c_str(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_PUT, ex.what(), "HPUT", table_name.c_str());
			}
	}
}

///////////////////////////////////////////////////////ERASE///////////////////////////////////////////////////////////

void CHbaseClientHelper::erase(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type delete_flag, uint64_t time_stamp) throw (CHbaseException)
{
    HBTable multi_row;
	multi_row.insert(std::make_pair(row_key,row));
	erase_multi(table_name, multi_row, delete_flag, time_stamp);
}

void CHbaseClientHelper::erase_multi(const std::string& table_name, const HBTable& rows, TDurability::type delete_flag, uint64_t time_stamp) throw (CHbaseException)
{
	if(rows.empty())
		return ;

	std::vector<apache::hadoop::hbase::thrift2::TDelete> del_vec;
	std::vector<apache::hadoop::hbase::thrift2::TDelete> del_vec_r;
	for(HBTable::const_iterator iter = rows.begin(); iter != rows.end(); iter ++)
	{
        //if(iter->m_option != DELETE)
        //    THROW_HBASE_EXCEPTION(HBASE_DELETE, "OPTION NOT RIGHT");

		apache::hadoop::hbase::thrift2::TDelete del;
		del.__set_row(iter->first);
		if(time_stamp > 0)
		{
			del.__set_timestamp(time_stamp);
		}

		std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;
		for(HBRow::const_iterator iter_ = iter->second.begin(); iter_ != iter->second.end(); iter_++)
		{
            std::vector<std::string> tokener;
            int ret = split(&tokener, iter_->first, ":");
            apache::hadoop::hbase::thrift2::TColumn  family_column;
            family_column.__set_family(tokener[0]);
            if(ret>1)
                family_column.__set_qualifier(tokener[1]);
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
					__HLOG_ERROR(ms_enable_log, "erase to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HDELETE", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "erase to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HDELETE", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "erase to %s exception: %s\n", table_name.c_str(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_DELETE, ex.what(), "HDELETE", table_name.c_str());
			}
	}
}
////////////////////////////////////////////////GET//////////////////////////////////////////////////////数据量和逻辑比较复杂

void CHbaseClientHelper::get(const std::string& table_name, const std::string& row_key, HBRow& row, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version) throw (CHbaseException)
{
    HBTable multi_row;
	multi_row.insert(std::make_pair(row_key,row));
	get_multi(table_name, multi_row, time_range, str_filter, max_version);
	row = multi_row[row_key];
}

void CHbaseClientHelper::get_multi(const std::string& table_name, HBTable& rows, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version) throw (CHbaseException)
{
	if(rows.empty())
		return ;

	std::vector<apache::hadoop::hbase::thrift2::TGet> get_vec;
	std::vector<apache::hadoop::hbase::thrift2::TResult> result;
	for(HBTable::const_iterator iter = rows.begin(); iter != rows.end(); iter ++)
	{
        //if(iter->m_option != GET)
        //    THROW_HBASE_EXCEPTION(HBASE_GET, "OPTION NOT RIGHT");

		apache::hadoop::hbase::thrift2::TGet get;
		get.__set_row(iter->first);

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
		for(HBRow::const_iterator iter_ = iter->second.begin(); iter_ != iter->second.end(); iter_++)
		{
            std::vector<std::string> tokener;
            int ret = split(&tokener, iter_->first, ":");
            apache::hadoop::hbase::thrift2::TColumn  family_column;
            family_column.__set_family(tokener[0]);
            if(ret>1)
                family_column.__set_qualifier(tokener[1]);
            family_columns.push_back(family_column);
		}
		get.__set_columns(family_columns);
		get_vec.push_back(get);
	}

	rows.clear();
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

				// 暂停清除
				// 10 行返回 9 行
				for(std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator iter = result.begin(); iter != result.end(); iter ++)
				{
					const std::string& row_key = iter->row;

					for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter_ = iter->columnValues.begin(); iter_ != iter->columnValues.end(); iter_++)
					{
						const std::string& family_name  = iter_->family;
						const std::string& column_name  = iter_->qualifier;
						const std::string& column_value = iter_->value;
						const uint64_t     column_timestamp = iter_->timestamp;
	                    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
						rows[row_key][cf] = HBCell(column_value, column_timestamp);
					}
				}
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
					__HLOG_ERROR(ms_enable_log, "get from to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGET", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGET", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s exception: %s\n", table_name.c_str(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_GET, ex.what(), "HGET", table_name.c_str());
			}
	}
}

void CHbaseClientHelper::get_by_scan(const std::string& table_name, const std::string& begin_row, const std::string& stop_row, const HBRow& column_family, HBTable& rows, uint16_t num_rows, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version) throw (CHbaseException)
{
	apache::hadoop::hbase::thrift2::TScan scan;
	scan.__set_startRow(begin_row);
	scan.__set_stopRow(stop_row);
	scan.__set_batchSize(num_rows);
	scan.__set_caching(1);   // 设置缓存时间？

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
    for(HBRow::const_iterator iter = column_family.begin(); iter!= column_family.end(); iter++)
    {
        std::vector<std::string> tokener;
        int ret = split(&tokener, iter->first, ":");
        apache::hadoop::hbase::thrift2::TColumn  family_column;
        family_column.__set_family(tokener[0]);
        if(ret>1)
            family_column.__set_qualifier(tokener[1]);
        family_columns.push_back(family_column);
    }
	scan.__set_columns(family_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
		try
		{
			std::vector<apache::hadoop::hbase::thrift2::TResult> results;
			(*m_hbase_client)->getScannerResults(results, table_name, scan, num_rows);   //number of size 是cell的size，不是row的size

			for(std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator iter = results.begin(); iter != results.end(); iter++)
			{
				const std::string& row_key = iter->row;

				for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter_ = iter->columnValues.begin(); iter_ != iter->columnValues.end(); iter_++)
				{
					const std::string& family_name = iter_->family;
					const std::string& column_name = iter_->qualifier;
					const std::string& column_value = iter_->value;
					const uint64_t column_timestamp = iter_->timestamp;
					std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
				    rows[row_key][cf] = HBCell(column_value, column_timestamp);
				}
			}
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
				__HLOG_ERROR(ms_enable_log, "get from %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGETSCAN", table_name.c_str());
			}

			if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
			{
				reconnect();
			}

		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "scan get from %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGETSCAN", table_name.c_str());
		}
		catch (apache::thrift::TException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "scan get from %s exception: %s\n", table_name.c_str(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_SCAN_GET, ex.what(), "HGETSCAN", table_name.c_str());
		}
	}
}

std::string CHbaseClientHelper::append(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, TDurability::type append_flag) throw (CHbaseException)
{
    HBRow   row;
    HBCell  cell;
    cell.m_value = column_value;
    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
    row.insert(std::make_pair(cf, cell));
    return append_multi(table_name, row_key, row, append_flag)[cf].m_value;
}

/////////////////////////////////////////////////////////////UPDATA////////////////////////////////////////////////
HBRow CHbaseClientHelper::append_multi(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type append_flag) throw (CHbaseException)
{
    HBRow row_;

	if(row.empty())
		return row_;

    //if(row.m_option != PUT)
    //    THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");

	//根据类型来判断 request 的类型
	apache::hadoop::hbase::thrift2::TAppend   append;
	std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_append_columns;

	append.__set_durability(append_flag);
	append.__set_row(row_key);
	for(HBRow::const_iterator iter = row.begin(); iter != row.end(); iter++)
	{
        std::vector<std::string> tokener;
        int ret = split(&tokener, iter->first, ":");
		apache::hadoop::hbase::thrift2::TColumnValue  family_column;
		family_column.__set_family(tokener[0]);
		if(ret>1)
		    family_column.__set_qualifier(tokener[1]);
		family_column.__set_value(iter->second.m_value);
		family_append_columns.push_back(family_column);
	}
	append.__set_columns(family_append_columns);

	const int retry_times = 2;
	for (int i=0; i<retry_times; ++i)
	{
			try
			{
				apache::hadoop::hbase::thrift2::TResult result;
				(*m_hbase_client)->append(result ,table_name, append);

                for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter = result.columnValues.begin(); iter != result.columnValues.end(); iter++)
                {
                    const std::string& family_name = iter->family;
                    const std::string& column_name = iter->qualifier;
                    const std::string& column_value = iter->value;
                    const uint64_t column_timestamp = iter->timestamp;
                    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
                    row_[cf] = HBCell(column_value, column_timestamp);
                }

                break;
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
					__HLOG_ERROR(ms_enable_log, "append to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HAPPEND", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "append to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HAPPEND", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "append to %s exception: %s\n", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_APPEND, ex.what(), "HAPPEND", table_name.c_str());
			}
	}

    return row_;
}

int64_t CHbaseClientHelper::increment(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, int64_t column_value, TDurability::type increment_flag) throw (CHbaseException)
{
    HBRow   row;
    HBCell  cell;
    cell.m_value = int_tostring(column_value);
    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
    row.insert(std::make_pair(cf, cell));
#if __WORDSIZE == 64
	  return atoll(increment_multi(table_name, row_key, row, increment_flag)[cf].m_value.c_str());
#else
	  return atol(increment_multi(table_name, row_key, row, increment_flag)[cf].m_value.c_str());
#endif
}


HBRow CHbaseClientHelper::increment_multi(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type increment_flag) throw (CHbaseException)
{
   // if(row.m_option != PUT)
   //     THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");
    HBRow row_;
	apache::hadoop::hbase::thrift2::TIncrement increment;
	std::vector<apache::hadoop::hbase::thrift2::TColumnIncrement> family_increment_columns;

	increment.__set_durability(increment_flag);
	increment.__set_row(row_key);
	for(HBRow::const_iterator iter = row.begin(); iter != row.end(); iter++)
	{
#if __WORDSIZE == 64
		int64_t inc64 =	atoll(iter->second.m_value.c_str());
#else
		int64_t inc64 =	atol(iter->second.m_value.c_str());
#endif
        std::vector<std::string> tokener;
        int ret = split(&tokener, iter->first, ":");
	    apache::hadoop::hbase::thrift2::TColumnIncrement  family_column;
	    family_column.__set_family(tokener[0]);
	    if(ret>1)
	        family_column.__set_qualifier(tokener[1]);
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

                for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter = result.columnValues.begin(); iter != result.columnValues.end(); iter++)
                {
                    const std::string& family_name = iter->family;
                    const std::string& column_name = iter->qualifier;
                    const std::string& column_value = iter->value;
                    const uint64_t column_timestamp = iter->timestamp;
                    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
                    int64_t i64_value =  bswap_64(*(int64_t*)(column_value.c_str()));
                    row_[cf] = HBCell(int_tostring(i64_value), column_timestamp);
                }

                break;
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
					__HLOG_ERROR(ms_enable_log, "increment to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
				    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HINCREMENT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "increment to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HINCREMENT", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "increment to %s exception: %s\n", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_INCREMENT, ex.what(), "HINCREMENT", table_name.c_str());
			}
	}

    return row_;
}

void  CHbaseClientHelper::check_and_put(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, const std::string& new_column_value, TDurability::type check_flag) throw (CHbaseException)
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
					__HLOG_ERROR(ms_enable_log, "check with replace %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
				    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with replace %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with replace %s exception: %s\n", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_CHECK_PUT, ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
			}
	}
}

void  CHbaseClientHelper::check_and_erase(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, TDurability::type check_flag) throw (CHbaseException)
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
					__HLOG_ERROR(ms_enable_log, "check with erase %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
				    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with erase %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
			}
			catch (apache::thrift::TException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "check with erase %s exception: %s\n", table_name.c_str(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HABSE_CHECK_DELEET, ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
			}
	}
}

void  CHbaseClientHelper::combination(const std::string& table_name, const HBOrder& rows_order, const HBTable& rows, TDurability::type options_flag, int64_t time_stamp) throw (CHbaseException)
{
    if(rows_order.empty() || rows.empty())
        return ;

    if(rows_order.size() != rows.size())
        THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "COMBINATION ORDER ERROR");

    std::string row_key_sameone;
    TRowMutations combs;
    std::vector<apache::hadoop::hbase::thrift2::TMutation> comb_vec;
    std::vector<apache::hadoop::hbase::thrift2::TResult> result;
    for(HBOrder::const_iterator iter = rows_order.begin(); iter != rows_order.end(); iter ++)
    {
        apache::hadoop::hbase::thrift2::TMutation comb;
        apache::hadoop::hbase::thrift2::TPut put;
        apache::hadoop::hbase::thrift2::TDelete del;
        if(!row_key_sameone.empty() && row_key_sameone != iter->first)
            THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "NO MORE ONE ROWKEY");
        else
            row_key_sameone = iter->first;

        if(iter->second == PUT)
        {
            put.__set_row(row_key_sameone);

            if(time_stamp > 0)
            {
                put.__set_timestamp(time_stamp);
            }

            std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_columns;
            HBTable::const_iterator rows_it = rows.find(iter->first);
            if(rows_it == rows.end())
                THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "NOT FIND ORDER KEY");

            for(HBRow::const_iterator iter_ = rows_it->second.begin(); iter_ != rows_it->second.end(); iter_++)
            {
                std::vector<std::string> tokener;
                int ret = split(&tokener, iter_->first, ":");
                apache::hadoop::hbase::thrift2::TColumnValue  family_column;
                family_column.__set_family(tokener[0]);
                if(ret>1)
                    family_column.__set_qualifier(tokener[1]);
                family_column.__set_value(iter_->second.m_value);
                family_columns.push_back(family_column);
            }

            put.__set_durability(options_flag);
            put.__set_columnValues(family_columns);
            comb.__set_put(put);
        }

        if(iter->second == DELETE)
        {
            del.__set_row(row_key_sameone);

            if(time_stamp > 0)
            {
                del.__set_timestamp(time_stamp);
            }

            HBTable::const_iterator rows_it = rows.find(iter->first);
            if(rows_it == rows.end())
                THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "NOT FIND ORDER ROW NAME");

            if(rows_it->second.size() > 1 || rows_it->second.empty())
                THROW_HBASE_EXCEPTION(HBASE_COMBINATION, "NOT DELETE COLUMN SINGLE");

            std::vector<apache::hadoop::hbase::thrift2::TColumn> family_columns;


            const ColumnFamily& cf = rows_it->second.begin()->first;
            std::vector<std::string> tokener;
            int ret = split(&tokener, cf, ":");
            apache::hadoop::hbase::thrift2::TColumn  family_column;
            family_column.__set_family(tokener[0]);
            if(ret>1)
                family_column.__set_qualifier(tokener[1]);
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
                    __HLOG_ERROR(ms_enable_log, "comb from to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCOMBINATION,", table_name.c_str());
                }

                if( apache::thrift::transport::TTransportException::TIMED_OUT == ex.getType())
                {
                    reconnect();
                }
            }
            catch (apache::thrift::TApplicationException& ex)
            {
                __HLOG_ERROR(ms_enable_log, "comb from %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCOMBINATION,", table_name.c_str());
            }
            catch (apache::thrift::TException& ex)
            {
                __HLOG_ERROR(ms_enable_log, "comb from %s exception: %s\n", table_name.c_str(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_COMBINATION, ex.what(), "HCOMBINATION,", table_name.c_str());
            }
    }
}

}
