/*
 * common.h
 *  基础工具方法
 *  Created on: 2016年11月29日
 *      Author: shuntan
 */
#ifndef HBASE_THRIFT2_COMMON_H_
#define HBASE_THRIFT2_COMMON_H_
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sstream>
#include <sys/time.h>
#include <stdlib.h>

#define __HLOG_ERROR(status, format, ...) \
{  \
	if(status)	\
	{	\
		printf("[HB-ERROR][%s:%d]", __FILE__, __LINE__); \
		printf(format, ##__VA_ARGS__); \
	}	\
}

#define __HLOG_INFO(status, format, ...) \
{  \
	if(status)	\
	{	\
		printf("[HB-INFO][%s:%d]", __FILE__, __LINE__); \
		printf(format, ##__VA_ARGS__); \
	}	\
}

#define __HLOG_DEBUG(status, format, ...) \
{  \
	if(status)	\
	{	\
		printf("[HB-DEBUG][%s:%d]", __FILE__, __LINE__); \
		printf(format, ##__VA_ARGS__); \
	}	\
}


namespace common{

extern uint16_t crc16(const char *buf, int len);

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
std::string int_tostring(T num)
{
	std::ostringstream stream;
	stream<<num;
	return stream.str();
}

}
#endif /* HBASE_THRIFT2_COMMON_H_ */
