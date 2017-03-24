#writer by shuntan@Tencent.com on 2016-11-25

cmake_minimum_required(VERSION 2.8)
project(hbase_client)

#set(THRIFT_HOME /usr/local/thirdparty/thrift)
#set(BOOST_HOME /usr/local/thirdparty/boost)

# 编译参数
# 启用__STDC_FORMAT_MACROS是为了可以使用inttypes.h中的PRId64等
# 启用__STDC_LIMIT_MACROS是为了可以使用stdint.h中的__UINT64_C和INT32_MIN等
add_definitions("-Wall -fPIC -pthread -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS")

# 设置依赖库路径
#thrift库
include_directories(${THRIFT_HOME}/include)
link_directories(${THRIFT_HOME}/lib)

#需要boost库下的动态库(thrift依赖boost)
include_directories(${BOOST_HOME}/include)
link_directories(${BOOST_HOME}/lib)

link_libraries(dl pthread rt z)

# 编译thrift
exec_program(${THRIFT_HOME}/bin/thrift ARGS --gen cpp -r -out ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/hbase.thrift)

# 生成 HBase helper 库
link_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(HBASE_SRC hbase_client_helper.cpp hbase_constants.cpp hbase_types.cpp THBaseService.cpp common.cpp)
add_library(hbase_client_helper STATIC ${HBASE_SRC})

# 使用例子
add_executable(client_test client_test.cpp)
target_link_libraries(client_test libhbase_client_helper.a libthriftnb.a libthrift.a)   

# 设置依赖关系：hbase_client_helper 库需要最先被编译
ADD_DEPENDENCIES(client_test hbase_client_helper)

# CMAKE_INSTALL_PREFIX
install(
        TARGETS hbase_client_helper
        DESTINATION lib
       )