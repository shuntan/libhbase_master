c-hbase-client 是c++版本基于hbase的RPC协议(thrift2)提供的非原生接口实现的一套API接口，具有较高可用性的客户端辅助类.

hbase.thrift 为官方提供的接口文件,c++,Java,PHP,...通用。
源代码http://apache.fayea.com/hbase/1.2.4/

编译链接c-habse-client库时，默认认为thrift(0.9.2)的安装目录为/usr/local/thirdparty/thrift, boost的安装目录为/usr/local/thirdparty/boost,可在CMakeLists.txt里修改。

编译c-habse-client成功后，将生成libhbase_client_helper.a静态库，没有共享库被生成。
也可以直接将里面的日志方法替换成自己的日志类，ip族初始化用Zookeeper接口代替。

编译c-habse-client（Compile c-habse-client）：
要求安装cmake且版本为2.8以上
$shell ： cmake. && make

安装（PREFIX指定安装目录，如果不指定则为/usr/local）：
make install
或（or）
make install PREFIX=/usr/local/r3c

执行单元测试：
client_test ： hbase提供的 thrift2 非原生接口测试。

生成源代码间的依赖：
thrift，boost，libevent

关于接口：
如果传给CHbaseClientHelper的nodes参数为单个节点字符串，如127.0.0.1:9090则为单机模式，为多节点字符串时则为随机访问模式。