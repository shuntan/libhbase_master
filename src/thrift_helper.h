/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: jian yi, eyjian@qq.com or eyjian@gmail.com
 * ���Լ�thrift��ʹ��
 * ע������boost��thrift�Ľӿ�����ʹ�õ�boost::shared_ptr
 */
#ifndef _THRIFT_HELPER_H
#define _THRIFT_HELPER_H
#include <arpa/inet.h>
#include <boost/scoped_ptr.hpp>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TSocketPool.h>
#include <thrift/transport/TTransportException.h>

// �����ж�thrift�Ƿ��Ѿ����ӣ��������������
// 1.��δ���ӹ���Ҳ���ǻ�δ�򿪹�����
// 2.���ӱ��Զ˹ر���
inline bool thrift_not_connected(
        apache::thrift::transport::TTransportException::TTransportExceptionType type)
{
    return (apache::thrift::transport::TTransportException::NOT_OPEN == type)
        || (apache::thrift::transport::TTransportException::END_OF_FILE == type);
}

inline bool thrift_not_connected(
        apache::thrift::transport::TTransportException& ex)
{
    apache::thrift::transport::TTransportException::TTransportExceptionType type = ex.getType();
    return thrift_not_connected(type);
}

// thrift�ͻ��˸�����
//
// ʹ��ʾ����
// mooon::net::CThriftClientHelper<ExampleServiceClient> client(rpc_server_ip, rpc_server_port);
// try
// {
//     client.connect();
//     client->foo();
// }
// catch (apache::thrift::transport::TTransportException& ex)
// {
//     //MYLOG_ERROR("thrift exception: (%d)%s\n", ex.getType(), ex.what());
// }
// catch (apache::thrift::transport::TApplicationException& ex)
// {
//     //MYLOG_ERROR("thrift exception: %s\n", ex.what());
// }
// catch (apache::thrift::TException& ex)
// {
//     //MYLOG_ERROR("thrift exception: %s\n", ex.what());
// }
// Transport��Ĭ�ϵ�TFramedTransport (TBufferTransports.h)������ѡ��
// TBufferedTransport (TBufferTransports.h)
// THttpTransport
// TZlibTransport
// TFDTransport (TSimpleFileTransport)
//
// Protocol��Ĭ�ϵ�apache::thrift::protocol::TBinaryProtocol������ѡ��
// TCompactProtocol
// TJSONProtocol
// TDebugProtocol
template <class ThriftClient,
          class Protocol=apache::thrift::protocol::TBinaryProtocol,
          class Transport=apache::thrift::transport::TFramedTransport>
class CThriftClientHelper
{
public:
    // host thrift����˵�IP��ַ
    // port thrift����˵Ķ˿ں�
    // connect_timeout_milliseconds ����thrift����˵ĳ�ʱ������
    // receive_timeout_milliseconds ����thrift����˷����������ݵĳ�ʱ������
    // send_timeout_milliseconds ��thrift����˷�������ʱ�ĳ�ʱ������
    CThriftClientHelper(const std::string &host, uint16_t port,
                        int connect_timeout_milliseconds=2000,
                        int receive_timeout_milliseconds=2000,
                        int send_timeout_milliseconds=2000);
    ~CThriftClientHelper();

    // ����thrift�����
    //
    // ����ʱ�����׳����¼���thrift�쳣��
    // apache::thrift::transport::TTransportException
    // apache::thrift::TApplicationException
    // apache::thrift::TException
    void connect();
    bool is_connected() const;

    // �Ͽ���thrift����˵�����
    //
    // ����ʱ�����׳����¼���thrift�쳣��
    // apache::thrift::transport::TTransportException
    // apache::thrift::TApplicationException
    // apache::thrift::TException
    void close();

    ThriftClient* get() { return _client.get(); }
    ThriftClient* get() const { return _client.get(); }
    ThriftClient* operator ->() { return get(); }
    ThriftClient* operator ->() const { return get(); }

    // ȡthrift����˵�IP��ַ
    const std::string& get_host() const { return _host; }
    // ȡthrift����˵Ķ˿ں�
    uint16_t get_port() const { return _port; }

    // ���ؿɶ��ı�ʶ�������ڼ�¼��־
    std::string str() const
    {
		char acTmp[64];
		memset(acTmp, 0, sizeof(acTmp));
		sprintf(acTmp,"thrift://%s:%u", _host.c_str(), _port);

		return std::string(acTmp);
        //return utils::CStringUtils::format_string("thrift://%s:%u", _host.c_str(), _port);
    }

private:
    std::string _host;
    uint16_t _port;
    boost::shared_ptr<apache::thrift::transport::TSocketPool> _sock_pool;
    boost::shared_ptr<apache::thrift::transport::TTransport> _socket;
    boost::shared_ptr<apache::thrift::transport::TFramedTransport> _transport;
    boost::shared_ptr<apache::thrift::protocol::TProtocol> _protocol;
    boost::shared_ptr<ThriftClient> _client;
};

////////////////////////////////////////////////////////////////////////////////
// thrift����˸�����
//
// ʹ��ʾ����
// mooon::net::CThriftServerHelper<CExampleHandler, ExampleServiceProcessor> _thrift_server;
// try
// {
//     _thrift_server.serve(listen_port);
// }
// catch (apache::thrift::TException& ex)
// {
//     //MYLOG_ERROR("thrift exception: %s\n", ex.what());
// }
// ProtocolFactory����Ĭ�ϵ�TBinaryProtocolFactory������ѡ��
// TCompactProtocolFactory
// TJSONProtocolFactory
// TDebugProtocolFactory
//
// ֻ֧��TNonblockingServerһ��Server
template <class ThriftHandler,
          class ServiceProcessor,
          class ProtocolFactory=apache::thrift::protocol::TBinaryProtocolFactory>
class CThriftServerHelper
{
public:
    // ����rpc������ע��õ�����ͬ�������ģ��������������
    // port thrift����˵ļ����˿ں�
    // num_threads thrift����˿������߳���
    //
    // ����ʱ�����׳����¼���thrift�쳣��
    // apache::thrift::transport::TTransportException
    // apache::thrift::TApplicationException
    // apache::thrift::TException
    // ����num_io_threads��ֻ�е�ServerΪTNonblockingServer����Ч
    void serve(uint16_t port, uint8_t num_worker_threads=1, uint8_t num_io_threads=1);
    void serve(const std::string &ip, uint16_t port, uint8_t num_worker_threads, uint8_t num_io_threads=1);
    void serve(const std::string &ip, uint16_t port, uint8_t num_worker_threads, uint8_t num_io_threads, void* attached);

    // Ҫ��ThriftHandler���з���attach(void*)
    void serve(uint16_t port, void* attached, uint8_t num_worker_threads=1, uint8_t num_io_threads=1);
    void stop();

    ThriftHandler* get()
    {
        return _handler.get();
    }
    ThriftHandler* get() const
    {
        return _handler.get();
    }

private:
    void init(const std::string &ip, uint16_t port, uint8_t num_worker_threads, uint8_t num_io_threads);

private:
    boost::shared_ptr<ThriftHandler> _handler;
    boost::shared_ptr<apache::thrift::TProcessor> _processor;
    boost::shared_ptr<apache::thrift::protocol::TProtocolFactory> _protocol_factory;
    boost::shared_ptr<apache::thrift::server::ThreadManager> _thread_manager;
    boost::shared_ptr<apache::thrift::concurrency::PosixThreadFactory> _thread_factory;
    boost::shared_ptr<apache::thrift::server::TServer> _server;
};

////////////////////////////////////////////////////////////////////////////////
// ��thrift�ص���д��־��������set_thrift_debug_log_function()������
inline void write_thrift_debug_log(const char* log)
{
    //__HLOG_DEBUG(::hbase::CHbaseClientHelper::ms_enable_log, "%s", log);
}

inline void write_thrift_info_log(const char* log)
{
   // __HLOG_INFO(::hbase::CHbaseClientHelper::ms_enable_log, "%s", log);
}

inline void write_thrift_error_log(const char* log)
{
   // __HLOG_ERROR(::hbase::CHbaseClientHelper::ms_enable_log, "%s", log);
}

// ��thrift���д�뵽��־�ļ���
inline void set_thrift_debug_log_function()
{
    //if (::mooon::sys::g_logger != NULL)
    apache::thrift::GlobalOutput.setOutputFunction(write_thrift_debug_log);
}

inline void set_thrift_info_log_function()
{
    //if (::mooon::sys::g_logger != NULL)
    apache::thrift::GlobalOutput.setOutputFunction(write_thrift_info_log);
}

inline void set_thrift_error_log_function()
{
    //if (::mooon::sys::g_logger != NULL)
    apache::thrift::GlobalOutput.setOutputFunction(write_thrift_error_log);
}

////////////////////////////////////////////////////////////////////////////////
template <class ThriftClient, class Protocol, class Transport>
CThriftClientHelper<ThriftClient, Protocol, Transport>::CThriftClientHelper(
        const std::string &host, uint16_t port,
        int connect_timeout_milliseconds, int receive_timeout_milliseconds, int send_timeout_milliseconds)
        : _host(host)
        , _port(port)
{
    set_thrift_debug_log_function();

    _sock_pool.reset(new apache::thrift::transport::TSocketPool());
    _sock_pool->addServer(host, (int)port);
    _sock_pool->setConnTimeout(connect_timeout_milliseconds);
    _sock_pool->setRecvTimeout(receive_timeout_milliseconds);
    _sock_pool->setSendTimeout(send_timeout_milliseconds);

    _socket = _sock_pool;
    // TransportĬ��Ϊapache::thrift::transport::TFramedTransport
    _transport.reset(new Transport(_socket));
    // ProtocolĬ��Ϊapache::thrift::protocol::TBinaryProtocol
    _protocol.reset(new Protocol(_transport));

    _client.reset(new ThriftClient(_protocol));
}

template <class ThriftClient, class Protocol, class Transport>
CThriftClientHelper<ThriftClient, Protocol, Transport>::~CThriftClientHelper()
{
    close();
}

template <class ThriftClient, class Protocol, class Transport>
void CThriftClientHelper<ThriftClient, Protocol, Transport>::connect()
{
    if (!_transport->isOpen())
    {
        _transport->open();
    }
}

template <class ThriftClient, class Protocol, class Transport>
bool CThriftClientHelper<ThriftClient, Protocol, Transport>::is_connected() const
{
    return _transport->isOpen();
}

template <class ThriftClient, class Protocol, class Transport>
void CThriftClientHelper<ThriftClient, Protocol, Transport>::close()
{
    if (_transport->isOpen())
    {
        _transport->close();
    }
}

////////////////////////////////////////////////////////////////////////////////
template <class ThriftHandler, class ServiceProcessor, class ProtocolFactory>
void CThriftServerHelper<ThriftHandler, ServiceProcessor, ProtocolFactory>::serve(uint16_t port, uint8_t num_worker_threads, uint8_t num_io_threads)
{
    serve("0.0.0.0", port, num_worker_threads, num_io_threads);
}

template <class ThriftHandler, class ServiceProcessor, class ProtocolFactory>
void CThriftServerHelper<ThriftHandler, ServiceProcessor, ProtocolFactory>::serve(const std::string &ip, uint16_t port, uint8_t num_worker_threads, uint8_t num_io_threads)
{
    init("0.0.0.0", port, num_worker_threads, num_io_threads);

    // ����Ҳ��ֱ�ӵ���serve()�����Ƽ�run()
    // !!!ע�����run()�Ľ��̻��̻߳ᱻ����
    _server->run();
}

template <class ThriftHandler, class ServiceProcessor, class ProtocolFactory>
void CThriftServerHelper<ThriftHandler, ServiceProcessor, ProtocolFactory>::serve(const std::string &ip, uint16_t port, uint8_t num_worker_threads, uint8_t num_io_threads, void* attached)
{
    init(ip, port, num_worker_threads, num_io_threads);

    // ����
    if (attached != NULL)
        _handler->attach(attached);

    // ����Ҳ��ֱ�ӵ���serve()�����Ƽ�run()
    // !!!ע�����run()�Ľ��̻��̻߳ᱻ����
    _server->run();
}

template <class ThriftHandler, class ServiceProcessor, class ProtocolFactory>
void CThriftServerHelper<ThriftHandler, ServiceProcessor, ProtocolFactory>::serve(uint16_t port, void* attached, uint8_t num_worker_threads, uint8_t num_io_threads)
{
    init("0.0.0.0", port, num_worker_threads, num_io_threads);

    // ����
    if (attached != NULL)
        _handler->attach(attached);

    // ����Ҳ��ֱ�ӵ���serve()�����Ƽ�run()
    // !!!ע�����run()�Ľ��̻��̻߳ᱻ����
    _server->run();
}

template <class ThriftHandler, class ServiceProcessor, class ProtocolFactory>
void CThriftServerHelper<ThriftHandler, ServiceProcessor, ProtocolFactory>::stop()
{
    _server->stop();
}

template <class ThriftHandler, class ServiceProcessor, class ProtocolFactory>
void CThriftServerHelper<ThriftHandler, ServiceProcessor, ProtocolFactory>::init(const std::string &ip, uint16_t port, uint8_t num_worker_threads, uint8_t num_io_threads)
{
    set_thrift_debug_log_function();

    _handler.reset(new ThriftHandler);
    _processor.reset(new ServiceProcessor(_handler));

    // ProtocolFactoryĬ��Ϊapache::thrift::protocol::TBinaryProtocolFactory
    _protocol_factory.reset(new ProtocolFactory());
    _thread_manager = apache::thrift::server::ThreadManager::newSimpleThreadManager(num_worker_threads);
    _thread_factory.reset(new apache::thrift::concurrency::PosixThreadFactory());

    _thread_manager->threadFactory(_thread_factory);
    _thread_manager->start();

    apache::thrift::server::TNonblockingServer* server = new apache::thrift::server::TNonblockingServer(_processor, _protocol_factory, port, _thread_manager);
    server->setNumIOThreads(num_io_threads);
    _server.reset(server);

    // ��Ҫ����_server->run()������serve()�����ã�
    // ��Ϊһ��������run()�󣬵����̻߳���̾ͱ������ˡ�
}

#endif // MOOON_NET_THRIFT_HELPER_H
