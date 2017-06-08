#writer by shuntan@Tencent.com on 2016-11-25

cmake_minimum_required(VERSION 2.8)
project(hbase_client)

# 让make时显示编译命令
set(CMAKE_VERBOSE_MAKEFILE ON)

# 定义颜色值，message()时可用到
if (NOT WIN32)
    string(ASCII 27 Esc)
    set(ColourReset "${Esc}[m")
    set(ColourBold  "${Esc}[1m")
    set(Red         "${Esc}[31m")
    set(Green       "${Esc}[32m")
    set(Yellow      "${Esc}[33m")
    set(Blue        "${Esc}[34m")
    set(Magenta     "${Esc}[35m")
    set(Cyan        "${Esc}[36m")
    set(White       "${Esc}[37m")
    set(BoldRed     "${Esc}[1;31m")
    set(BoldGreen   "${Esc}[1;32m")
    set(BoldYellow  "${Esc}[1;33m")
    set(BoldBlue    "${Esc}[1;34m")
    set(BoldMagenta "${Esc}[1;35m")
    set(BoldCyan    "${Esc}[1;36m")
    set(BoldWhite   "${Esc}[1;37m")
endif ()

# 编译参数
# 启用__STDC_FORMAT_MACROS是为了可以使用inttypes.h中的PRId64等
# 启用__STDC_LIMIT_MACROS是为了可以使用stdint.h中的__UINT64_C和INT32_MIN等
add_definitions("-Wall -fPIC -pthread -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS")

# 搜索库的目录函数
function(discover_library uppername dirname)
    if (${uppername}_HOME)
        if (EXISTS ${${uppername}_HOME})
            set(HAVE_${uppername} 1 CACHE INTERNAL HAVE_${uppername})
        else ()
            set(HAVE_${uppername} 0 CACHE INTERNAL HAVE_${uppername})
        endif ()
    elseif (EXISTS /usr/local/thirdparty/${dirname})
        # 如果使用PARENT_SCOPE，则函数内不能识别，但函数外可以
        #set(HAVE_${uppername} 1 PARENT_SCOPE)
        set(HAVE_${uppername} 1 CACHE INTERNAL HAVE_${uppername})
        set(${uppername}_HOME /usr/local/thirdparty/${dirname} CACHE INTERNAL ${uppername}_HOME)
    elseif (EXISTS /usr/local/${dirname})
        set(HAVE_${uppername} 1 CACHE INTERNAL HAVE_${uppername})
        set(${uppername}_HOME /usr/local/${dirname} CACHE INTERNAL ${uppername}_HOME)
    elseif (EXISTS $ENV{HOME}/${dirname})
        #set(HAVE_${uppername} 1 PARENT_SCOPE CACHE INTERNAL HAVE_${uppername})
        set(HAVE_${uppername} 1 CACHE INTERNAL HAVE_${uppername})
        set(${uppername}_HOME $ENV{HOME}/${dirname} CACHE INTERNAL ${uppername}_HOME)
    else ()
        set(HAVE_${uppername} 0 CACHE INTERNAL HAVE_${uppername})
    endif ()

    if (NOT ${HAVE_${uppername}})
        if (${uppername}_HOME)
            message("${Red}-- not found ${uppername} in ${uppername}_HOME${ColourReset}")
        else ()
            message("${Red}-- not found ${uppername}${ColourReset}")
        endif ()
    else ()
        message("${Green}-- ${uppername} found in ${${uppername}_HOME}${ColourReset}")
        add_definitions("-DHAVE_${uppername}=1")
        include_directories(${${uppername}_HOME}/include)
        link_directories(${${uppername}_HOME}/lib)
    endif ()
endfunction ()

# 搜寻THRIFT库
discover_library(THRIFT thrift)
if (HAVE_THRIFT)    
    include_directories(${THRIFT_HOME}/include)
    link_directories(${THRIFT_HOME}/lib)
endif ()

# 搜寻BOOST库
discover_library(BOOST boost)
if (HAVE_BOOST)    
    include_directories(${BOOST_HOME}/include)
    link_directories(${BOOST_HOME}/lib)
endif ()

# 检查event.h头文件是否拥有
if (EXISTS /usr/include/event.h)        
    message("${Green}-- event.h found in /usr/include${ColourReset}")
else ()
    message("${Red}-- not found event.h in /usr/include${ColourReset}")
    message("${Yellow}-- copy event.h to /usr/include/${ColourReset}")
    set(COPYITEM event.h)  
    set(EXECUTABLE_OUTPUT_PATH /usr/include)
    execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/${COPYITEM} ${EXECUTABLE_OUTPUT_PATH}/${COPYITEM}  )    
endif ()

link_libraries(dl pthread rt z)

# 编译thrift
message("${Cyan}")
exec_program(${THRIFT_HOME}/bin/thrift ARGS --gen cpp -r -out ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/hbase.thrift)
message("${ColourReset}")

# 生成 HBase helper 库
link_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(HBASE_SRC hbase_helper.cpp hbase_constants.cpp hbase_types.cpp THBaseService.cpp)
add_library(hbase++ STATIC ${HBASE_SRC})

# 使用例子(1)
add_executable(client_test client_test.cpp)
target_link_libraries(client_test libhbase++.a libthriftnb.a libthrift.a)   

# 单元测试(2)
#add_executable(test test.cpp)

# 设置依赖关系：hbase++ 库需要最先被编译
ADD_DEPENDENCIES(client_test hbase++)

# CMAKE_INSTALL_PREFIX
install(
        TARGETS hbase++
        DESTINATION lib
       )
install(
        FILES hbase_helper.h THBaseService.h hbase_types.h hbase_constants.h
        DESTINATION include
       )