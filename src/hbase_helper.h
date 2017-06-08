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
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TDenseProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TSocketPool.h>
#include <thrift/transport/TTransportException.h>
#include "THBaseService.h"

using namespace apache::hadoop::hbase::thrift2;

namespace hbase{

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
    COMPACT = 1,     //
    JSON = 2,
    DENSE = 3,
    DEBUG = 4
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
//static void thrift_log(const char* log);

template <class ThriftClient>
class CThriftClientHelper
{
public:
    CThriftClientHelper(
            const std::string &host, uint16_t port,
            int connect_timeout_milliseconds=2000,
            int receive_timeout_milliseconds=2000,
            int send_timeout_milliseconds=2000,
            TProtocolType protocol_type= BINARY,
            TransportType transport_type= FRAMED);

    ~CThriftClientHelper();

    void connect();
    bool is_connected() const;
    void close();

    ThriftClient* get();
    ThriftClient* get() const;
    ThriftClient* operator ->();
    ThriftClient* operator ->() const;

    const std::string& get_host()const;
    uint16_t get_port() const;
    std::string str() const;

private:
    std::string m_host;
    uint16_t m_port;
    boost::shared_ptr<apache::thrift::transport::TSocketPool> m_sock_pool;
    boost::shared_ptr<apache::thrift::transport::TTransport> m_socket;
    boost::shared_ptr<apache::thrift::transport::TTransport> m_transport;
    boost::shared_ptr<apache::thrift::protocol::TProtocol> m_protocol;
    boost::shared_ptr<ThriftClient> m_client;
};

/////////////////////////////////CHbaseException//////////////////////////////////////

class CHbaseException: public std::exception
{
public:
    CHbaseException(int errcode, const std::string& errmsg, const char* file, int line, const std::string& node_ip = "", int16_t node_port = 0, const char* command=NULL, const char* key=NULL) throw ();
    virtual ~CHbaseException() throw () {}
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
    uint64_t     m_time_stamp;

    HBCell(): m_time_stamp(0)
    {

    }

    HBCell(const std::string& value, uint64_t time_stamp)
    {
        m_value = value;
        m_time_stamp = time_stamp;
    }

    struct HBCell& operator = (const std::string& value)
    {
        m_value = value;
        return *this;
    }

    struct HBCell& operator = (const HBCell& cell)
    {
        m_value = cell.m_value;
        m_time_stamp = cell.m_time_stamp;
        return *this;
    }

    bool empty()
    {
        return m_value.empty() && m_time_stamp == 0;
    }
} HBCell_t;

typedef std::string RowKey;
typedef std::string ColumnFamily;
typedef std::pair<uint64_t, uint64_t> HBTimeRange;       //时间戳范围
typedef std::map<ColumnFamily, HBCell> HBRow;
typedef std::map<RowKey, HBRow> HBTable;
typedef std::vector<std::pair<RowKey, OptionType> > HBOrder;

///////////////////////////////////////////////////////CHbaseClientHelper/////////////////////////////////////////////////

class CHbaseClientHelper
{
public:
	//  输入IP:PORT 列表 e.g., 127.0.0.1:6379,127.0.0.1:6380,127.0.0.2:6379,127.0.0.3:6379,
	CHbaseClientHelper(const std::string& host_list, uint32_t connect_timeout = 2000, uint32_t recive_timeout = 2000, uint32_t send_time_out = 2000) throw (CHbaseException);
	~CHbaseClientHelper();

	//	返回单例对象或者指针, CHbaseClientHelper::Get_Singleton() -OR- Get_SingletonPtr()
	static CHbaseClientHelper&  get_singleton(const std::string& host_list, uint32_t connect_timeout = 2000, uint32_t recive_timeout = 2000, uint32_t send_time_out = 2000) throw (CHbaseException);
	static CHbaseClientHelper*  get_singleton_ptr(const std::string& host_list, uint32_t connect_timeout = 2000, uint32_t recive_timeout = 2000, uint32_t send_time_out = 2000) throw (CHbaseException);
	//  是否开启日志提醒, CHbaseClientHelper::Ignore_Log(); --忽略日志
	static void  ignore_log();

    bool  connect() throw (CHbaseException);
    bool  reconnect(bool random = false) throw (CHbaseException);
    void  close() throw (CHbaseException);

public:
    void  update(const char* format, ...) throw (CHbaseException) __attribute__((format(printf, 2, 3)));
    void  query(HBTable* rows, const char* format, ...) throw (CHbaseException) __attribute__((format(printf, 3, 4)));
    void  query(HBRow* row, const char* format, ...) throw (CHbaseException) __attribute__((format(printf, 3, 4)));
    std::string query(const char* format, ...) throw (CHbaseException) __attribute__((format(printf, 2, 3)));

public:
	//exists：检查表内是否存在某行或某行内某些列，输入是表名、TGet，输出是bool
	bool  exist(const std::string& table_name, const std::string& row_key, const HBRow& row) throw (CHbaseException);

	//对某一行内增加若干列，输入是表名，TPut结构
	void  put(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type insert_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);
	//putMultiple：对put的扩展，一次增加若干行内的若个列，输入是表名、TPut数组
	void  put_multi(const std::string& table_name, const HBTable& rows, TDurability::type insert_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);

	//删除某一行内增加若干列，输入是表名，TDelete结构
	void  erase(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type delete_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);
	//deleteMultiple：对delete的扩展，一次增加若干行内的若个列，输入是表名、TDelete数组
	void  erase_multi(const std::string& table_name, const HBTable& rows, TDurability::type delete_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);

	//对某一行内的查询，输入是表名、TGet结构，输出是TResult
	void  get(const std::string& table_name, const std::string& row_key, HBRow& row, HBTimeRange* time_range = NULL, const std::string& str_filter = "", uint16_t max_version = 0) throw (CHbaseException);
	//getMultiple：实际上是对get的扩展，输入是表名、TGet数组，输出是TResult数组
	void  get_multi(const std::string& table_name, HBTable& row_list, HBTimeRange* time_range = NULL, const std::string& str_filter = "", uint16_t max_version = 0) throw (CHbaseException);
	//查询的条件由TScan封装，在打开时传入。需要注意的是每次取数据的行数要合适，否则有效率问题。
	void  get_by_scan(const std::string& table_name, const std::string& begin_row, const std::string& stop_row, const HBRow& column_family, HBTable& rows, uint16_t num_rows, HBTimeRange* time_range = NULL, const std::string& str_filter = "", uint16_t max_version = 0) throw (CHbaseException);

	//对某行中若干列进行追加内容.
    std::string append(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& column_value, TDurability::type append_flag=TDurability::FSYNC_WAL) throw (CHbaseException);

	HBRow append_multi(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type append_flag=TDurability::FSYNC_WAL) throw (CHbaseException);

	//增加一行内某列的值，这个操作比较特别，是专门用于计数的，也保证了“原子”操作特性。
	int64_t increment(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, int64_t column_value, TDurability::type increment_flag=TDurability::FSYNC_WAL) throw (CHbaseException);
	//增加一行内某些列的值，这个操作比较特别，是专门用于计数的，也保证了“原子”操作特性。
	HBRow increment_multi(const std::string& table_name, const std::string& row_key, const HBRow& row, TDurability::type increment_flag=TDurability::FSYNC_WAL) throw (CHbaseException);

	//当传入的（表名+列族名+列名+新数据+老数据）都存在于数据库时，才做操作
	void  check_and_put(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, const std::string& new_column_value, TDurability::type check_flag=TDurability::FSYNC_WAL) throw (CHbaseException);
	//当传入的（表名+列族名+列名+数据）都存在于数据库时，才做操作
	void  check_and_erase(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, TDurability::type check_flag=TDurability::FSYNC_WAL) throw (CHbaseException);

	// 联合操作 包括多个put 和  多个delete
	void  combination(const std::string& table_name, const HBOrder& rows_order,const HBTable& rows, TDurability::type options_flag=TDurability::FSYNC_WAL, int64_t time_stamp = 0) throw (CHbaseException);

public:
    static bool ms_enable_log;

private:
    void* get_random_service();
    template <typename T> T hbase_shell(const std::string& habse_shell) throw (CHbaseException);

private:
	std::vector<boost::shared_ptr<void> > m_hbase_clients;
	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* m_hbase_client;
};

} // namespace end of habse

#endif /* HBASE_THRIFT_SHUNTAN_HBASE_CLIENT_HELPER_H_ */
