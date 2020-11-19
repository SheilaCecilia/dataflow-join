cmake_minimum_required(VERSION 3.18)
project(CountLabeledQuery)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES count_vertex_labeled_query.cpp plan.hpp)
add_executable(CountLabeledQuery ${SOURCE_FILES})

if (DEFINED ENV{BOOST_ROOT})
    set(BOOST_ROOT $ENV{BOOST_ROOT})
endif ()
set(Boost_NO_BOOST_CMAKE true)
find_package(Boost REQUIRED COMPONENTS filesystem system thread)
include_directories(${Boost_INCLUDE_DIRS})
set(ExtLibs ${ExtLibs} ${Boost_LIBRARIES})

