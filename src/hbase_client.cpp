/*
 * hbase_client_helper.cpp
 *
 *  Created on: 2016年11月19日
 *      Author: shuntan
 */
#include "hbase_client.h"

#ifdef __GNUC__
#  define UNUSED(x) UNUSED_ ## x __attribute__((__unused__))
#else
#  define UNUSED(x) UNUSED_ ## x
#endif

//异常捕获类
#define THROW_HBASE_EXCEPTION(errcode, errmsg) throw CHBaseException(errcode, errmsg, __FILE__, __LINE__)
#define THROW_HBASE_EXCEPTION_WITH_COMMAND(errcode, errmsg, command, key) throw CHBaseException(errcode, errmsg, __FILE__, __LINE__, std::string(""), 0, command, key)
#define THROW_HBASE_EXCEPTION_WITH_NODE(errcode, errmsg, node_ip, node_port) throw CHBaseException(errcode, errmsg, __FILE__, __LINE__, node_ip, node_port)
#define THROW_HBASE_EXCEPTION_WITH_NODE_AND_COMMAND(errcode, errmsg, node_ip, node_port, command, key) throw CHBaseException(errcode, errmsg, __FILE__, __LINE__, node_ip, node_port, command, key)

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


namespace hbase{ namespace thrift2{

bool CHBaseClient::ms_enable_log = true;

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

void thrift_log(const char* log)
{
    __HLOG_DEBUG(CHBaseClient::ms_enable_log, "%s", log);
}

///////////////////////////////////////CThriftClientHelper/////////////////////////////////////////

template <class ThriftClient>
CThriftClientHelper<ThriftClient>::CThriftClientHelper(
        const std::string &host, uint16_t port,
        int connect_timeout_milliseconds, int receive_timeout_milliseconds, int send_timeout_milliseconds)
        : _connect_timeout_milliseconds(connect_timeout_milliseconds),
          _receive_timeout_milliseconds(receive_timeout_milliseconds),
          _send_timeout_milliseconds(send_timeout_milliseconds)
{
    apache::thrift::GlobalOutput.setOutputFunction(thrift_log);
    _socket.reset(new apache::thrift::transport::TSocket(host, port));
    init();
}

template <class ThriftClient>
CThriftClientHelper<ThriftClient>::CThriftClientHelper(
        const std::vector<std::pair<std::string, int> >& servers,
        int connect_timeout_milliseconds,
        int receive_timeout_milliseconds,
        int send_timeout_milliseconds,
        int num_retries, int retry_interval,
        int max_consecutive_failures,
        bool randomize, bool always_try_last)
        : _connect_timeout_milliseconds(connect_timeout_milliseconds),
          _receive_timeout_milliseconds(receive_timeout_milliseconds),
          _send_timeout_milliseconds(send_timeout_milliseconds)
{
    apache::thrift::GlobalOutput.setOutputFunction(thrift_log);
    apache::thrift::transport::TSocketPool* socket_pool = new apache::thrift::transport::TSocketPool(servers);
    socket_pool->setNumRetries(num_retries);
    socket_pool->setRetryInterval(retry_interval);
    socket_pool->setMaxConsecutiveFailures(max_consecutive_failures);
    socket_pool->setRandomize(randomize);
    socket_pool->setAlwaysTryLast(always_try_last);
    _socket.reset(socket_pool);
    init();
}

template <class ThriftClient>
void CThriftClientHelper<ThriftClient>::init()
{
    _socket->setConnTimeout(_connect_timeout_milliseconds);
    _socket->setRecvTimeout(_receive_timeout_milliseconds);
    _socket->setSendTimeout(_send_timeout_milliseconds);

    // Transport默认为apache::thrift::transport::TFramedTransport
    _transport.reset(new apache::thrift::transport::TFramedTransport(_socket));
    // Protocol默认为apache::thrift::protocol::TBinaryProtocol
    _protocol.reset(new apache::thrift::protocol::TBinaryProtocol(_transport));
    // 服务端的Client
    _client.reset(new ThriftClient(_protocol));

    dynamic_cast<apache::thrift::protocol::TBinaryProtocol*>(_protocol.get())->setStringSizeLimit(DEFAULT_MAX_STRING_SIZE);
    dynamic_cast<apache::thrift::protocol::TBinaryProtocol*>(_protocol.get())->setStrict(false, false);
}

template <class ThriftClient>
CThriftClientHelper<ThriftClient>::~CThriftClientHelper()
{
    close();
}

template <class ThriftClient>
void CThriftClientHelper<ThriftClient>::connect()
{
    if (!_transport->isOpen())
    {
        // 如果Transport为TFramedTransport，则实际调用：TFramedTransport::open -> TSocketPool::open
        _transport->open();
        // 当"TSocketPool::open: all connections failed"时，
        // TSocketPool::open就抛出异常TTransportException，异常类型为TTransportException::NOT_OPEN
    }
}

template <class ThriftClient>
bool CThriftClientHelper<ThriftClient>::is_connected() const
{
    return _transport->isOpen();
}

template <class ThriftClient>
void CThriftClientHelper<ThriftClient>::close()
{
    if (_transport->isOpen())
    {
        _transport->close();
    }
}

template <class ThriftClient>
std::string CThriftClientHelper<ThriftClient>::get_host() const
{
    return _socket->getHost();
}

template <class ThriftClient>
uint16_t CThriftClientHelper<ThriftClient>::get_port() const
{
    return static_cast<uint16_t>(_socket->getPort());
}

//////////////////////////////CHbaseException/////////////////////////////////

CHBaseException::CHBaseException(int errcode, const std::string& errmsg, const char* file, int line, const std::string& node_ip, int16_t node_port, const char* command, const char* key) throw ()
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

inline const char* CHBaseException::what() const throw()
{
    return m_errmsg.c_str();
}

std::string CHBaseException::str() const throw ()
{
    if(m_node_port > 0 && m_node_ip.size() > 8)
        return format_string("HBASE://%s:%d/%s/%s/(%d):%s@%s:%d", m_node_ip.c_str(), m_node_port, m_command.c_str(), m_key.c_str(), m_errcode, m_errmsg.c_str(), m_file.c_str(), m_line);
    else
        return format_string("HBASE://%s/%s/(%d):%s@%s:%d", m_command.c_str(), m_key.c_str(), m_errcode, m_errmsg.c_str(), m_file.c_str(), m_line);
}

////////////////////////////////////////////CHbaseClientHelper///////////////////////////////////////////////////////////

CHBaseClient::CHBaseClient(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out, uint8_t retry_times) //  throw (CHBaseException)
    : m_retry_times(retry_times)
{
	std::vector<std::string> host_array;
	split(&host_array, host_list, ",");
	std::vector<std::pair<std::string, int> > servers;
	for(std::vector<std::string>::const_iterator iter = host_array.begin(); iter != host_array.end(); iter++)
	{
		std::vector<std::string> ip_port;
		split(&ip_port, *iter, ":");
		if(ip_port.size() != 2)
		{
			continue;
		}
		const std::string& host_ip   = ip_port[0];
		const std::string& host_port = ip_port[1];
		servers.push_back(std::make_pair(host_ip, atoi(host_port.c_str())));
	}

	if(servers.size() < 1)
	{
		__HLOG_ERROR(ms_enable_log, "HBase service hosts empty! \n");
		THROW_HBASE_EXCEPTION(HOST_ERROR, "HBase service hosts empty");
	}
	else
	{
	    m_hbase_clients.reset(new CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>(servers, connect_timeout, recive_timeout, send_time_out));
		connect();
	}
}

CHBaseClient& CHBaseClient::get_singleton(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out)
{
    return *get_singleton_ptr(host_list, connect_timeout, recive_timeout, send_time_out);
}

CHBaseClient* CHBaseClient::get_singleton_ptr(const std::string& host_list, uint32_t connect_timeout, uint32_t recive_timeout, uint32_t send_time_out)
{
    static __thread CHBaseClient* s_singleton = NULL;

    try
    {
        if(!s_singleton)
        {
            s_singleton = new CHBaseClient(host_list, connect_timeout, recive_timeout, send_time_out);
        }
    }
    catch(CHBaseException& ex)
    {
        delete s_singleton;
        return NULL;
    }

    return s_singleton;
}

void CHBaseClient::ignore_log()
{
    ms_enable_log = false;
}

CHBaseClient::~CHBaseClient()
{
    close();
}

template <typename T> T CHBaseClient::hbase_shell(const std::string& habse_shell)//  throw (CHBaseException)
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

bool CHBaseClient::connect()//  throw (CHBaseException)
{
    try
    {
        m_hbase_clients.get()->connect();
    }
    catch (apache::thrift::transport::TTransportException& ex)
    {
        __HLOG_INFO(ms_enable_log, "connect hbase://%s:%u, transport(I/O) exception: (%d)(%s)\n", m_hbase_clients->get_host().c_str(), m_hbase_clients.get()->get_port(), ex.getType(), ex.what());
        THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_clients->get_host(), m_hbase_clients.get()->get_port());
    }
    catch (apache::thrift::TApplicationException& ex)
    {
        __HLOG_ERROR(ms_enable_log, "connect hbase://%s:%u application exception: %s\n",m_hbase_clients.get()->get_host().c_str(), m_hbase_clients.get()->get_port(), ex.what());
        THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_clients.get()->get_host(), m_hbase_clients.get()->get_port());
    }

    return true;
}

bool CHBaseClient::reconnect()//  throw (CHBaseException)
{
    close();
    return connect();
}

void CHBaseClient::close()//  throw (CHBaseException)
{
    try
    {
        m_hbase_clients.get()->close();
    }
    catch (apache::thrift::transport::TTransportException& ex)
    {
        __HLOG_INFO(ms_enable_log, "connect hbase://%s:%u, transport(I/O) exception: (%d)(%s)\n", m_hbase_clients->get_host().c_str(), m_hbase_clients.get()->get_port(), ex.getType(), ex.what());
        THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_clients->get_host(), m_hbase_clients.get()->get_port());
    }
    catch (apache::thrift::TApplicationException& ex)
    {
        __HLOG_ERROR(ms_enable_log, "connect hbase://%s:%u application exception: %s\n",m_hbase_clients.get()->get_host().c_str(), m_hbase_clients.get()->get_port(), ex.what());
        THROW_HBASE_EXCEPTION_WITH_NODE(ex.getType(), ex.what(), m_hbase_clients.get()->get_host(), m_hbase_clients.get()->get_port());
    }
}

void CHBaseClient::update(const char* format, ...)//  throw (CHBaseException)
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

void CHBaseClient::query(HBTable* rows, const char* format, ...)//  throw (CHBaseException)
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

void CHBaseClient::query(HBRow* row, const char* format, ...)//  throw (CHBaseException)
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

std::string CHBaseClient::query(const char* format, ...)//  throw (CHBaseException)
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

bool CHBaseClient::exist(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name)//  throw (CHBaseException)
{
    HBRow   row;
    HBCell  cell;
    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
    row.insert(std::make_pair(cf, cell));
    return exist(table_name, row_key, row);
}

bool CHBaseClient::exist(const std::string& table_name, const std::string& row_key, const HBRow& row)//  throw (CHBaseException)
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

	uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
		try
		{
			bool bResult;
			bResult = (*m_hbase_clients)->exists(table_name, get);
			return bResult;
		}
	    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
	    {
	        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
	        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HEXISTS", table_name.c_str());
	    }
		catch (apache::thrift::transport::TTransportException& ex)
		{
			if ( times - 1 == i)
			{
				__HLOG_ERROR(ms_enable_log, "exists %s transport exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
				THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HEXISTS", table_name.c_str());
			}

			if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
			{
			    reconnect();
			}
		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "exists %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HEXISTS", table_name.c_str());
		}
		catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
		{
			__HLOG_ERROR(ms_enable_log, "exists %s exception: %s\n", table_name.c_str(), ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_EXIST, ex.message.c_str(), "HEXISTS", table_name.c_str());
		}
        catch(apache::thrift::protocol::TProtocolException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "exists %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HEXISTS", table_name.c_str());
        }
	}

	return false;
}

/////////////////////////////////////////////////////INSERT////////////////////////////////////////////////////////////////////

void CHBaseClient::put(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability, uint64_t time_stamp)//  throw (CHBaseException)
{
    HBTable multi_row;
	multi_row.insert(std::make_pair(row_key,row));
	put(table_name, multi_row, durability, time_stamp);
}

void CHBaseClient::put(const std::string& table_name, const HBTable& rows, TDurability::type durability, uint64_t time_stamp)//  throw (CHBaseException)
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
		put.__set_durability(durability);
		put_vec.push_back(put);
	}

	uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
			try
			{
				if(put_vec.size() > 1)
				    (*m_hbase_clients)->putMultiple(table_name, put_vec);
				else
				    (*m_hbase_clients)->put(table_name, put_vec.front());
			}
		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HPUT", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log,	 "put to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HPUT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
				{
				    reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "put to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HPUT", table_name.c_str());
			}
			catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
			{
				__HLOG_ERROR(ms_enable_log, "put to %s exception: %s\n", table_name.c_str(), ex.message.c_str());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_PUT, ex.message.c_str(), "HPUT", table_name.c_str());
			}
            catch(apache::thrift::protocol::TProtocolException& ex)
            {
                __HLOG_ERROR(ms_enable_log, "put to %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HPUT", table_name.c_str());
            }
	}
}

///////////////////////////////////////////////////////ERASE///////////////////////////////////////////////////////////

void CHBaseClient::erase(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability, uint64_t time_stamp)//  throw (CHBaseException)
{
    HBTable multi_row;
	multi_row.insert(std::make_pair(row_key,row));
	erase(table_name, multi_row, durability, time_stamp);
}

void CHBaseClient::erase(const std::string& table_name, const HBTable& rows, TDurability::type durability, uint64_t time_stamp)//  throw (CHBaseException)
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
		del.__set_durability(durability);
		del_vec.push_back(del);
	}

	uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
			try
			{
				if(del_vec.size() > 1)
				    (*m_hbase_clients)->deleteMultiple(del_vec_r, table_name, del_vec);
				else
				    (*m_hbase_clients)->deleteSingle(table_name, del_vec.front());
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HDELETE", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "erase to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HDELETE", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "erase to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HDELETE", table_name.c_str());
			}
			catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
			{
				__HLOG_ERROR(ms_enable_log, "erase to %s exception: %s\n", table_name.c_str(), ex.message.c_str());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_DELETE, ex.message.c_str(), "HDELETE", table_name.c_str());
			}
            catch(apache::thrift::protocol::TProtocolException& ex)
            {
                __HLOG_ERROR(ms_enable_log, "erase to %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HDELETE", table_name.c_str());
            }
	}
}
////////////////////////////////////////////////GET//////////////////////////////////////////////////////数据量和逻辑比较复杂

void CHBaseClient::get(const std::string& table_name, const std::string& row_key, HBRow& row, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version)//  throw (CHBaseException)
{
    HBTable multi_row;
	multi_row.insert(std::make_pair(row_key,row));
	get(table_name, multi_row, time_range, str_filter, max_version);
	row = multi_row[row_key];
}

void CHBaseClient::get(const std::string& table_name, HBTable& rows, HBTimeRange* time_range, const std::string& str_filter, uint16_t max_version)//  throw (CHBaseException)
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

    uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
			try
			{
				if(get_vec.size() > 1)
				    (*m_hbase_clients)->getMultiple(result, table_name, get_vec);
				else
				{
					apache::hadoop::hbase::thrift2::TResult get_r;
					(*m_hbase_clients)->get(get_r, table_name, get_vec.front());
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
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HGET", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "get from to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGET", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGET", table_name.c_str());
			}
			catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s exception: %s\n", table_name.c_str(), ex.message.c_str());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_GET, ex.message.c_str(), "HGET", table_name.c_str());
			}
	        catch(apache::thrift::protocol::TProtocolException& ex)
	        {
	            __HLOG_ERROR(ms_enable_log, "get from %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGET", table_name.c_str());
	        }
	}
}

void CHBaseClient::get(const std::string& table_name, const std::string& begin_row, const std::string& stop_row, const HBRow& row, HBTable& rows, uint16_t num_rows, HBTimeRange* time_range, bool rev, const std::string& str_filter, uint16_t max_version)//  throw (CHBaseException)
{
	apache::hadoop::hbase::thrift2::TScan scan;
	scan.__set_startRow(begin_row);
	scan.__set_stopRow(stop_row);
	/// scan.__set_batchSize(num_rows);
	scan.__set_caching(static_cast<int32_t>(num_rows*row.size()));

	if(!str_filter.empty() )
		scan.__set_filterString(str_filter);

	if(rev)
	    scan.__set_reversed(true);

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
    for(HBRow::const_iterator iter = row.begin(); iter!= row.end(); iter++)
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

    uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
		try
		{
			std::vector<apache::hadoop::hbase::thrift2::TResult> results;
			(*m_hbase_clients)->getScannerResults(results, table_name, scan, num_rows);   //number of size 是cell的size，不是row的size

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
	        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
	        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HGETSCAN", table_name.c_str());
	    }
		catch (apache::thrift::transport::TTransportException& ex)
		{
			if ( times - 1 == i)
			{
				__HLOG_ERROR(ms_enable_log, "get from %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGETSCAN", table_name.c_str());
			}

			if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
			{
				reconnect();
			}

		}
		catch (apache::thrift::TApplicationException& ex)
		{
			__HLOG_ERROR(ms_enable_log, "scan get from %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGETSCAN", table_name.c_str());
		}
		catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
		{
			__HLOG_ERROR(ms_enable_log, "scan get from %s exception: %s\n", table_name.c_str(), ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_SCAN_GET, ex.message.c_str(), "HGETSCAN", table_name.c_str());
		}
        catch(apache::thrift::protocol::TProtocolException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "scan get from %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HGETSCAN", table_name.c_str());
        }
	}
}

std::string CHBaseClient::append(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, TDurability::type durability)//  throw (CHBaseException)
{
    HBRow   row;
    HBCell  cell;
    cell.m_value = column_value;
    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
    row.insert(std::make_pair(cf, cell));
    return append(table_name, row_key, row, durability)[cf].m_value;
}

/////////////////////////////////////////////////////////////UPDATA////////////////////////////////////////////////
HBRow CHBaseClient::append(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability)//  throw (CHBaseException)
{
    HBRow row_;

	if(row.empty())
		return row_;

    //if(row.m_option != PUT)
    //    THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");

	//根据类型来判断 request 的类型
	apache::hadoop::hbase::thrift2::TAppend   append;
	std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_append_columns;

	append.__set_durability(durability);
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

    uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
			try
			{
				apache::hadoop::hbase::thrift2::TResult result;
				(*m_hbase_clients)->append(result ,table_name, append);

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
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HAPPEND", table_name.c_str());
		    }
			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "append to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
		            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HAPPEND", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "append to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HAPPEND", table_name.c_str());
			}
			catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
			{
				__HLOG_ERROR(ms_enable_log, "append to %s exception: %s\n", table_name.c_str(), ex.message.c_str());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_APPEND, ex.message.c_str(), "HAPPEND", table_name.c_str());
			}
            catch(apache::thrift::protocol::TProtocolException& ex)
            {
                __HLOG_ERROR(ms_enable_log, "append to %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HAPPEND", table_name.c_str());
            }
	}

    return row_;
}

std::string CHBaseClient::increment(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, TDurability::type durability)//  throw (CHBaseException)
{
    HBRow   row;
    HBCell  cell;
    cell.m_value = column_value;
    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
    row.insert(std::make_pair(cf, cell));
    /*
#if __WORDSIZE == 64
	  return atoll(increment_multi(table_name, row_key, row, increment_flag)[cf].m_value.c_str());
#else
	  return atol(increment_multi(table_name, row_key, row, increment_flag)[cf].m_value.c_str());
#endif
    */
    return increment(table_name, row_key, row, durability)[cf].m_value;
}


HBRow CHBaseClient::increment(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability)//  throw (CHBaseException)
{
   // if(row.m_option != PUT)
   //     THROW_HBASE_EXCEPTION(HBASE_PUT, "OPTION NOT RIGHT");
    HBRow row_;
	apache::hadoop::hbase::thrift2::TIncrement increment;
	std::vector<apache::hadoop::hbase::thrift2::TColumnIncrement> family_increment_columns;

	increment.__set_durability(durability);
	increment.__set_row(row_key);
	for(HBRow::const_iterator iter = row.begin(); iter != row.end(); iter++)
	{
//#if __WORDSIZE == 64
		int64_t inc64 =	atoll(iter->second.m_value.c_str());
//#else
//		int64_t inc64 =	atol(iter->second.m_value.c_str());
//#endif
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

    uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
			try
			{
				apache::hadoop::hbase::thrift2::TResult result;
				(*m_hbase_clients)->increment(result ,table_name, increment);

                for(std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator iter = result.columnValues.begin(); iter != result.columnValues.end(); iter++)
                {
                    const std::string& family_name = iter->family;
                    const std::string& column_name = iter->qualifier;
                    const std::string& column_value = iter->value;
                    const uint64_t column_timestamp = iter->timestamp;
                    std::string cf = family_name + (column_name.empty() ? column_name : ":" + column_name);
                    int64_t i64_value =  bswap_64(*(int64_t*)(column_value.data()));
                    row_[cf] = HBCell(int_tostring(i64_value), column_timestamp);
                }

                break;
			}

		    catch (apache::hadoop::hbase::thrift2::TIOError& ex)
		    {
		        __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
		        THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HINCREMENT", table_name.c_str());
		    }

			catch (apache::thrift::transport::TTransportException& ex)
			{
				if ( times - 1 == i)
				{
					__HLOG_ERROR(ms_enable_log, "increment to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
				    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HINCREMENT", table_name.c_str());
				}

				if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
				{
					reconnect();
				}
			}
			catch (apache::thrift::TApplicationException& ex)
			{
				__HLOG_ERROR(ms_enable_log, "increment to %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HINCREMENT", table_name.c_str());
			}
			catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
			{
				__HLOG_ERROR(ms_enable_log, "increment to %s exception: %s\n", table_name.c_str(), ex.message.c_str());
			    THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_INCREMENT, ex.message.c_str(), "HINCREMENT", table_name.c_str());
			}
	        catch(apache::thrift::protocol::TProtocolException& ex)
	        {
	            __HLOG_ERROR(ms_enable_log, "increment to %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
	            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HINCREMENT", table_name.c_str());
	        }
	}

    return row_;
}

bool  CHBaseClient::check_and_put(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, const HBRow& row, TDurability::type durability, uint64_t time_stamp)//  throw (CHBaseException)
{
	apache::hadoop::hbase::thrift2::TPut put;
	put.__set_row(row_key);
	if(time_stamp>0)
	    put.__set_timestamp(time_stamp);
	std::vector<apache::hadoop::hbase::thrift2::TColumnValue> family_columns;
    for(HBRow::const_iterator iter = row.begin(); iter != row.end(); iter++)
    {
        std::vector<std::string> tokener;
        int ret = split(&tokener, iter->first, ":");
        apache::hadoop::hbase::thrift2::TColumnValue  family_column;
        family_column.__set_family(tokener[0]);
        if(ret>1)
            family_column.__set_qualifier(tokener[1]);
        family_column.__set_value(iter->second.m_value);
        family_columns.push_back(family_column);
    }
	put.__set_columnValues(family_columns);
	put.__set_durability(durability);

    uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
        try
        {
            return (*m_hbase_clients)->checkAndPut(table_name, row_key, family_name, column_name, column_value, put);
        }
        catch (apache::hadoop::hbase::thrift2::TIOError& ex)
        {
            __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HCHECK_WITH_PUT", table_name.c_str());
        }
        catch (apache::thrift::transport::TTransportException& ex)
        {
            if ( times - 1 == i)
            {
                __HLOG_ERROR(ms_enable_log, "check with replace %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
            }

            if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
            {
                reconnect();
            }
        }
        catch (apache::thrift::TApplicationException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "check with replace %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
        }
        catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
        {
            __HLOG_ERROR(ms_enable_log, "check with replace %s exception: %s\n", table_name.c_str(), ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_CHECK_PUT, ex.message.c_str(), "HCHECK_WITH_PUT", table_name.c_str());
        }
        catch(apache::thrift::protocol::TProtocolException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "check with replace %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_PUT", table_name.c_str());
        }
	}

	return false;
}

bool  CHBaseClient::check_and_erase(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, const HBRow& row, TDurability::type durability, uint64_t time_stamp)//  throw (CHBaseException)
{
	apache::hadoop::hbase::thrift2::TDelete del;
	del.__set_row(row_key);
    if(time_stamp>0)
        del.__set_timestamp(time_stamp);
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
    del.__set_deleteType(TDeleteType::DELETE_COLUMN);
    del.__set_columns(family_columns);
    del.__set_durability(durability);

    uint8_t times = m_retry_times + 1;
	for (uint8_t i=0; i<times; ++i)
	{
        try
        {
            return (*m_hbase_clients)->checkAndDelete(table_name, row_key, family_name, column_name ,column_value, del);
        }
        catch (apache::hadoop::hbase::thrift2::TIOError& ex)
        {
            __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HCHECK_WITH_DELETE", table_name.c_str());
        }
        catch (apache::thrift::transport::TTransportException& ex)
        {
            if ( times - 1 == i)
            {
                __HLOG_ERROR(ms_enable_log, "check with erase %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
            }

            if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
            {
                reconnect();
            }
        }
        catch (apache::thrift::TApplicationException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "check with erase %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
        }
        catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
        {
            __HLOG_ERROR(ms_enable_log, "check with erase %s exception: %s\n", table_name.c_str(), ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HABSE_CHECK_DELEET, ex.message.c_str(), "HCHECK_WITH_DELETE", table_name.c_str());
        }
        catch(apache::thrift::protocol::TProtocolException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "check with erase %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCHECK_WITH_DELETE", table_name.c_str());
        }
	}

	return false;
}

void  CHBaseClient::combination(const std::string& table_name, const HBOrder& rows_order, const HBTable& rows, TDurability::type durability, int64_t time_stamp)//  throw (CHBaseException)
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

            put.__set_durability(durability);
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
            del.__set_durability(durability);
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

    uint8_t times = m_retry_times + 1;
    for (uint8_t i=0; i<times; ++i)
    {
        try
        {
            (*m_hbase_clients)->mutateRow(table_name, combs);
        }

        catch (apache::hadoop::hbase::thrift2::TIOError& ex)
        {
            __HLOG_ERROR(ms_enable_log, "IOError: %s\n", ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(IO_ERROR, ex.message.c_str(), "HCOMBINATION,", table_name.c_str());
        }
        catch (apache::thrift::transport::TTransportException& ex)
        {
            if ( times - 1 == i)
            {
                __HLOG_ERROR(ms_enable_log, "comb from to %s transport exception(I/0): (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
                THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCOMBINATION,", table_name.c_str());
            }

            if( apache::thrift::transport::TTransportException::NOT_OPEN == ex.getType())
            {
                reconnect();
            }
        }
        catch (apache::thrift::TApplicationException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "comb from %s application exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCOMBINATION,", table_name.c_str());
        }
        catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)
        {
            __HLOG_ERROR(ms_enable_log, "comb from %s exception: %s\n", table_name.c_str(), ex.message.c_str());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(HBASE_COMBINATION, ex.message.c_str(), "HCOMBINATION,", table_name.c_str());
        }
        catch(apache::thrift::protocol::TProtocolException& ex)
        {
            __HLOG_ERROR(ms_enable_log, "comb from %s protocol exception: (%d)%s\n", table_name.c_str(), ex.getType(), ex.what());
            THROW_HBASE_EXCEPTION_WITH_COMMAND(ex.getType(), ex.what(), "HCOMBINATION", table_name.c_str());
        }
    }
}

}}
