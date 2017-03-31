/*
 * hbase_client_helper.h
 *
 *  Created on: 2016年11月29日
 *      Author: shuntan
 */

#ifndef HBASE_THRIFT_SHUNTAN_HBASE_CLIENT_HELPER_H_
#define HBASE_THRIFT_SHUNTAN_HBASE_CLIENT_HELPER_H_
#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <sstream>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <boost/make_shared.hpp>
#include "thrift_helper.h"
#include "THBaseService.h"

using namespace apache::hadoop::hbase::thrift2;

namespace hbase{

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

template <class ContainerType>
inline int split(ContainerType* tokens, const std::string& source, const std::string& sep, bool skip_sep=false)
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
inline T get_random_number(unsigned int i, T max_number)
{
    struct timeval tv;
    struct timezone *tz = NULL;

    gettimeofday(&tv, tz);
    srandom(tv.tv_usec + i); // 加入i，以解决过快时tv_usec值相同

    // RAND_MAX 类似于INT_MAX
    return static_cast<T>(random() % max_number);
}

template <typename T>
inline std::string int_tostring(T num)
{
	std::ostringstream stream;
	stream<<num;
	return stream.str();
}

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

typedef struct TCell
{
	std::string  m_family;
	std::string  m_qualifier;
	std::string  m_value;
	uint64_t     m_time_stamp;

	TCell(): m_family(""), m_qualifier(""), m_value(""),  m_time_stamp(0){}
	TCell(const std::string& family): m_family(family), m_qualifier(""), m_value(""), m_time_stamp(0){}
	TCell(const std::string& family, const std::string& qualifier):m_family(family), m_qualifier(qualifier), m_value(""), m_time_stamp(0){}
	TCell(const std::string& family, const std::string& qualifier, const std::string& value):m_family(family), m_qualifier(qualifier), m_value(value), m_time_stamp(0){}
	TCell(const std::string& family, const std::string& qualifier, const std::string& value, uint64_t timestamp):m_family(family), m_qualifier(qualifier), m_value(value), m_time_stamp(timestamp){}
} TCell_t;

typedef std::pair<uint64_t, uint64_t> HBTimeRange;       //时间戳范围
typedef struct TRow
{
	OptionType  m_option;                                //操作属性 -- /增/删/取
	std::string m_row_key;                               //行键值
	std::vector<TCell> m_cells;                          //若干个列族

	TRow()
	{
	    m_option = (OptionType)0;
	}

	TRow(const OptionType& option_type)
	{
	    m_option = option_type;
	}

	/*
	std::string& operator[](const std::string& family, const std::string& qualifier = "", const uint64_t& timestamp = 0)
    {
	    std::vector<TCell>::iterator iter;
        for(iter = m_cells.begin(); iter != m_cells.end(); iter++)
        {
            if(iter->m_family == family && iter->m_qualifier == qualifier)
                return iter->m_value;
        }

        return m_cells.insert(iter, TCell(family, qualifier, "", timestamp))->m_value;
    }
    */

	//设置行的键值
	void set_rowkey(const std::string& row)
	{
		m_row_key = row;
	}

	//增加一个族下的列名
	void add_column(const std::string& family, const std::string& qualifier = "")
	{
	    if(qualifier.empty())
	        m_cells.push_back(TCell(family));
	    else
	        m_cells.push_back(TCell(family, qualifier));
	}
	/////////////////////////////////////////////服务端生成的时间戳/////////////////////////////////////////

	//一般类型
	void add_column_value(const std::string& family, const std::string& qualifier, const std::string& value, uint64_t timestamp = 0)
	{
	    if(timestamp > 0)
	        m_cells.push_back(TCell(family, qualifier, value, timestamp));
	    else
	        m_cells.push_back(TCell(family, qualifier, value));
	}

	//increment类型使用
	void add_column_value(const std::string& family, const std::string& qualifier, int64_t value, uint64_t timestamp = 0)
	{
	    if(timestamp > 0)
	        m_cells.push_back(TCell(family, qualifier, int_tostring(value), timestamp));
	    else
            m_cells.push_back(TCell(family, qualifier, int_tostring(value)));
	}

	const std::string& get_rowkey() const
	{
		return m_row_key;
	}

	const std::vector<TCell>& get_columns() const
	{
		return m_cells;
	}

	void reset(const OptionType& option_type = (OptionType)0)
	{
	    m_option = option_type;
		m_row_key.clear();
		m_cells.clear();
	}
} TRow_t;

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
	static void  ignore_log()
	{
		ms_enable_log = false;
	}

public:

	void* get_random_service();

	bool  connect() throw (CHbaseException);
	bool  reconnect(bool random = false) throw (CHbaseException);

	//exists：检查表内是否存在某行或某行内某些列，输入是表名、TGet，输出是bool
	bool  exist(const std::string& table_name, const TRow& row) throw (CHbaseException);

	//对某一行内增加若干列，输入是表名，TPut结构
	bool  put(const std::string& table_name, const TRow& row, TDurability::type insert_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);
	//putMultiple：对put的扩展，一次增加若干行内的若个列，输入是表名、TPut数组
	bool  put_multi(const std::string& table_name, const std::vector<TRow>& row_list, TDurability::type insert_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);

	//删除某一行内增加若干列，输入是表名，TDelete结构
	bool  erase(const std::string& table_name, const TRow& row, TDurability::type delete_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);
	//deleteMultiple：对delete的扩展，一次增加若干行内的若个列，输入是表名、TDelete数组
	bool  erase_multi(const std::string& table_name, const std::vector<TRow>& row_list, TDurability::type delete_flag=TDurability::FSYNC_WAL, uint64_t time_stamp = 0) throw (CHbaseException);

	//对某一行内的查询，输入是表名、TGet结构，输出是TResult
	bool  get(const std::string& table_name, TRow& row, HBTimeRange* time_range = NULL, const std::string& str_filter = "", uint16_t max_version = 0) throw (CHbaseException);
	//getMultiple：实际上是对get的扩展，输入是表名、TGet数组，输出是TResult数组
	bool  get_multi(const std::string& table_name, std::vector<TRow>& row_list, HBTimeRange* time_range = NULL, const std::string& str_filter = "", uint16_t max_version = 0) throw (CHbaseException);
	//查询的条件由TScan封装，在打开时传入。需要注意的是每次取数据的行数要合适，否则有效率问题。
	bool  get_by_scan(const std::string& table_name, const std::string& begin_row, const std::string& stop_row, std::vector<TRow>& row_list, uint16_t num_rows, HBTimeRange* time_range = NULL, const std::string& str_filter = "", uint16_t max_version = 0) throw (CHbaseException);

	//对某行中若干列进行追加内容.
	bool  append(const std::string& table_name, const TRow& row, TDurability::type append_flag=TDurability::FSYNC_WAL) throw (CHbaseException);

	//增加一行内某列的值，这个操作比较特别，是专门用于计数的，也保证了“原子”操作特性。
	bool  increment(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, int64_t column_value, TDurability::type increment_flag=TDurability::FSYNC_WAL) throw (CHbaseException);
	//增加一行内某些列的值，这个操作比较特别，是专门用于计数的，也保证了“原子”操作特性。
	bool  increment_multi(const std::string& table_name, const TRow& row, TDurability::type increment_flag=TDurability::FSYNC_WAL) throw (CHbaseException);

	//当传入的（表名+列族名+列名+新数据+老数据）都存在于数据库时，才做操作
	bool  check_and_put(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, const std::string& new_column_value, TDurability::type check_flag=TDurability::FSYNC_WAL) throw (CHbaseException);
	//当传入的（表名+列族名+列名+数据）都存在于数据库时，才做操作
	bool  check_and_erase(const std::string& table_name, const std::string& row_key, const std::string& family_name, const std::string& column_name, const std::string& old_column_value, TDurability::type check_flag=TDurability::FSYNC_WAL) throw (CHbaseException);

	// 联合操作 包括多个put 和  多个delete
	bool  combination(const std::string& table_name, const std::vector<TRow>& row_list, TDurability::type options_flag=TDurability::FSYNC_WAL, int64_t time_stamp = 0) throw (CHbaseException);

public:
    static bool ms_enable_log;

private:
	std::vector<boost::shared_ptr<CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient> > > m_hbase_clients;
	CThriftClientHelper<apache::hadoop::hbase::thrift2::THBaseServiceClient>* m_hbase_client;
};

} // namespace end of habse

#endif /* HBASE_THRIFT_SHUNTAN_HBASE_CLIENT_HELPER_H_ */
