/*
 * hbase_client_helper.h
 *
 *  Created on: 2016年11月29日
 *      Author: shuntan
 */

#ifndef __HBASE_HELPER_H
#define __HBASE_HELPER_H
#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <sstream>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <map>
#include <boost/make_shared.hpp>
#include <arpa/inet.h>
#include <boost/scoped_ptr.hpp>
#include <thrift/config.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocketPool.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportException.h>
#include "THBaseService.h"
//#include <thrift/concurrency/PosixThreadFactory.h>
//#include <thrift/concurrency/ThreadManager.h>
//#include <thrift/protocol/TCompactProtocol.h>
//#include <thrift/protocol/TJSONProtocol.h>
//#include <thrift/protocol/TDebugProtocol.h>
//#include <thrift/server/TNonblockingServer.h>

using namespace apache::hadoop::hbase::thrift2;

namespace hbase{ namespace thrift2{

/*
#define bswap_64(n) \
      ( (((n) & 0xff00000000000000ull) >> 56) \
      | (((n) & 0x00ff000000000000ull) >> 40) \
      | (((n) & 0x0000ff0000000000ull) >> 24) \
      | (((n) & 0x000000ff00000000ull) >> 8)  \
      | (((n) & 0x00000000ff000000ull) << 8)  \
      | (((n) & 0x0000000000ff0000ull) << 24) \
      | (((n) & 0x000000000000ff00ull) << 40) \
      | (((n) & 0x00000000000000ffull) << 56) )
*/

enum TransportType   //传输方式
{
    BUFFERED = 0,
    FRAMED = 1,
    //FILE   = 2
};

enum TProtocolType
{
    BINARY = 0,       //池子
    COMPACT = 1,      //
    JSON = 2,
    DEBUG = 3
};

enum OptionType
{
    PUT = 1,
    DELETE = 2,
    GET = 3
};

enum ErrorCode
{
    NO_ERROR = 0,
    HOST_ERROR = 1,
    CONNECT_FAILED = 2,
    IO_ERROR = 3,
    HBASE_EXIST = 4,
    HBASE_PUT = 5,
    HBASE_DELETE = 6,
    HBASE_GET = 7,
    HBASE_SCAN_GET = 8,
    HBASE_APPEND = 9,
    HBASE_INCREMENT = 10,
    HBASE_CHECK_PUT = 11,
    HABSE_CHECK_DELEET = 12,
    HBASE_COMBINATION = 13
};

/////////////////////////////////////CThriftClientHelper////////////////////////////////////////////
extern void thrift_log(const char* log);

//apache::thrift::protocol::TBinaryProtocol,apache::thrift::transport::TFramedTransport
template <class ThriftClient>
class CThriftClientHelper
{
public:
    static const int DEFAULT_MAX_STRING_SIZE = 256 * 1024 * 1024;
public:
    // host thrift服务端的IP地址
    // port thrift服务端的端口号
    // connect_timeout_milliseconds 连接thrift服务端的超时毫秒数
    // receive_timeout_milliseconds 接收thrift服务端发过来的数据的超时毫秒数
    // send_timeout_milliseconds 向thrift服务端发送数据时的超时毫秒数
    // set_log_function 是否设置写日志函数，默认设置为debug级别日志
    CThriftClientHelper(const std::string &host, uint16_t port,
                        int connect_timeout_milliseconds=2000,
                        int receive_timeout_milliseconds=2000,
                        int send_timeout_milliseconds=2000);

    // 支持指定多个servers，运行时随机选择一个，当一个异常时自动选择其它
    // num_retries 重试次数
    // retry_interval 重试间隔，单位为秒
    // max_consecutive_failures 单个Server最大连续失败次数
    // randomize_ 是否随机选择一个Server
    // always_try_last 是否总是重试最后一个Server
    // set_log_function 是否设置写日志函数，默认设置为debug级别日志
    CThriftClientHelper(const std::vector<std::pair<std::string, int> >& servers,
                        int connect_timeout_milliseconds=2000,
                        int receive_timeout_milliseconds=2000,
                        int send_timeout_milliseconds=2000,
                        int num_retries=1, int retry_interval=60,
                        int max_consecutive_failures=1,
                        bool randomize=true, bool always_try_last=true);
    ~CThriftClientHelper();

    // 连接thrift服务端
    //
    // 出错时，可抛出以下几个thrift异常：
    // apache::thrift::transport::TTransportException
    // apache::thrift::TApplicationException
    // apache::thrift::TException
    void connect();
    bool is_connected() const;

    // 断开与thrift服务端的连接
    //
    // 出错时，可抛出以下几个thrift异常：
    // apache::thrift::transport::TTransportException
    // apache::thrift::TApplicationException
    // apache::thrift::TException
    void close();

    apache::thrift::transport::TSocket* get_socket() { return _socket.get(); }
    const apache::thrift::transport::TSocket get_socket() const { return _socket.get(); }
    ThriftClient* get() { return _client.get(); }
    ThriftClient* get() const { return _client.get(); }
    ThriftClient* operator ->() { return get(); }
    ThriftClient* operator ->() const { return get(); }

    // 取thrift服务端的IP地址
    std::string get_host() const;
    // 取thrift服务端的端口号
    uint16_t get_port() const;

    // 返回可读的标识，常用于记录日志
    std::string str() const
    {
        return format_string("thrift://%s:%u", get_host().c_str(), get_port());
    }

private:
    void init();

private:
    int _connect_timeout_milliseconds;
    int _receive_timeout_milliseconds;
    int _send_timeout_milliseconds;

private:
    // TSocket只支持一个server，而TSocketPool是TSocket的子类支持指定多个server，运行时随机选择一个
    boost::shared_ptr<apache::thrift::transport::TSocket> _socket;
    boost::shared_ptr<apache::thrift::transport::TTransport> _transport;
    boost::shared_ptr<apache::thrift::protocol::TProtocol> _protocol;
    boost::shared_ptr<ThriftClient> _client;
};

/////////////////////////////////CHbaseException//////////////////////////////////////

class CHBaseException: public std::exception
{
public:
    CHBaseException(int errcode, const std::string& errmsg, const char* file, int line, const std::string& node_ip = "", int16_t node_port = 0, const char* command=NULL, const char* key=NULL) throw ();
    virtual ~CHBaseException() throw () {}
    virtual const char* what() const throw ();
    std::string str() const throw ();

    const char* file() const throw () { return m_file.c_str(); }
    int line() const throw () { return m_line; }
    const char* node_ip() const throw () { return m_node_ip.c_str(); }
    uint16_t node_port() const throw () { return m_node_port; }
    const char* command() const throw() { return m_command.c_str(); }
    const char* key() const throw() { return m_key.c_str(); }

private:
    const int m_errcode;
    const std::string m_errmsg;
    const std::string m_file;
    const int m_line;
    const std::string m_node_ip;
    const uint16_t m_node_port;
    std::string m_command;
    std::string m_key;
};

//////////////////////////////////// STRUCT HBCell //////////////////////////////////////////////
typedef struct HBCell
{
    std::string  m_value;
    uint64_t     m_timestamp;

    HBCell(): m_timestamp(0){}

    HBCell(const std::string& value, uint64_t timestamp)
    {
        m_value = value;
        m_timestamp = timestamp;
    }

    struct HBCell& operator = (const std::string& value)
    {
        m_value = value;
        return *this;
    }

    struct HBCell& operator = (const HBCell& cell)
    {
        m_value = cell.m_value;
        m_timestamp = cell.m_timestamp;
        return *this;
    }

    bool operator == (const std::string& value)
    {
        return m_value == value;
    }

    bool operator != (const std::string& value)
    {
        return m_value != value;
    }

    bool empty()
    {
        return m_value.empty() && m_timestamp == 0;
    }

} HBCell_t;

typedef std::string RowKey;
typedef std::string ColumnFamily;
typedef std::pair<uint64_t, uint64_t> HBTimeRange;       //时间戳范围
typedef std::map<ColumnFamily, HBCell> HBRow;
typedef std::map<RowKey, HBRow> HBTable;
typedef std::vector<std::pair<RowKey, OptionType> > HBOrder;

///////////////////////////////////////////////////////CHbaseClientHelper/////////////////////////////////////////////////
//TODO ASYNC_WAL ： 当数据变动时，异步写WAL日志
//TODO SYNC_WAL  ： 当数据变动时，同步写WAL日志
//TODO FSYNC_WAL ： 当数据变动时，同步写WAL日志，并且，强制将数据写入磁盘
//TODO SKIP_WAL  ： 不写WAL日志
//TODO USE_DEFAULT ： 使用HBase全局默认的WAL写入级别，即 SYNC_WAL
class CHBaseClient
{
public:
	//  输入IP:PORT 列表 e.g., 127.0.0.1:6379,127.0.0.1:6380,127.0.0.2:6379,127.0.0.3:6379,
    CHBaseClient(const std::string& host_list, uint32_t connect_timeout = 2000, uint32_t recive_timeout = 2000, uint32_t send_time_out = 2000, uint8_t retry_times = 0);// throw (CHBaseException);
	~CHBaseClient();

	//	返回单例对象或者指针, CHbaseClientHelper::Get_Singleton() -OR- Get_SingletonPtr()
	static CHBaseClient&  get_singleton(const std::string& host_list, uint32_t connect_timeout = 2000, uint32_t recive_timeout = 2000, uint32_t send_time_out = 2000);
	static CHBaseClient*  get_singleton_ptr(const std::string& host_list, uint32_t connect_timeout = 2000, uint32_t recive_timeout = 2000, uint32_t send_time_out = 2000);
	//  是否开启日志提醒, CHbaseClientHelper::Ignore_Log(); --忽略日志
	static void  ignore_log();

    bool  connect();// throw (CHBaseException);
    bool  reconnect();// throw (CHBaseException);
    void  close();// throw (CHBaseException);

public:
    void  update(const char* format, ...);// throw (CHBaseException) __attribute__((format(printf, 2, 3)));
    void  query(HBTable* rows, const char* format, ...);// throw (CHBaseException) __attribute__((format(printf, 3, 4)));
    void  query(HBRow* row, const char* format, ...);// throw (CHBaseException) __attribute__((format(printf, 3, 4)));
    std::string query(const char* format, ...);// throw (CHBaseException) __attribute__((format(printf, 2, 3)));

public:
	bool  exist(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name);// throw (CHBaseException);
	bool  exist(const std::string& table_name, const std::string& row_key, const HBRow& row);// throw (CHBaseException);

	void  put(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability=TDurability::SYNC_WAL, uint64_t time_stamp=0);// throw (CHBaseException);
	void  put(const std::string& table_name, const HBTable& rows, TDurability::type durability=TDurability::SYNC_WAL, uint64_t time_stamp=0);// throw (CHBaseException);

	void  erase(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability=TDurability::SYNC_WAL, uint64_t time_stamp=0);// throw (CHBaseException);
	void  erase(const std::string& table_name, const HBTable& rows, TDurability::type durability=TDurability::SYNC_WAL, uint64_t time_stamp=0);// throw (CHBaseException);

	void  get(const std::string& table_name, const std::string& row_key, HBRow& row, HBTimeRange* time_range = NULL, const std::string& str_filter="", uint16_t max_version=0);// throw (CHBaseException);
	void  get(const std::string& table_name, HBTable& row_list, HBTimeRange* time_range = NULL, const std::string& str_filter="", uint16_t max_version=0);// throw (CHBaseException);
	void  get(const std::string& table_name, const std::string& begin_row, const std::string& stop_row, const HBRow& row, HBTable& rows, uint16_t num_rows, HBTimeRange* time_range=NULL, const std::string& str_filter="", uint16_t max_version=0);// throw (CHBaseException);

    std::string append(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, TDurability::type durability=TDurability::SYNC_WAL);// throw (CHBaseException);
	HBRow append(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability=TDurability::SYNC_WAL);// throw (CHBaseException);

	std::string increment(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, TDurability::type durability=TDurability::SYNC_WAL);// throw (CHBaseException);
	HBRow increment(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type durability=TDurability::SYNC_WAL);// throw (CHBaseException);

	bool  check_and_put(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, const HBRow& row, TDurability::type durability=TDurability::SYNC_WAL, uint64_t time_stamp=0);// throw (CHBaseException);
	bool  check_and_erase(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, const HBRow& row, TDurability::type durability=TDurability::SYNC_WAL, uint64_t time_stamp=0);// throw (CHBaseException);

	void  combination(const std::string& table_name, const HBOrder& rows_order, const HBTable& rows, TDurability::type durability=TDurability::SYNC_WAL, int64_t time_stamp=0);// throw (CHBaseException);

public:
    static bool ms_enable_log;

private:
    template <typename T> T hbase_shell(const std::string& hbase_shell);// throw (CHBaseException);

private:
	boost::shared_ptr<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient> >  m_hbase_clients;
	const uint8_t m_retry_times;
};

}} // namespace end of hbase

#endif /* HBASE_THRIFT_SHUNTAN_HBASE_CLIENT_HELPER_H_ */
