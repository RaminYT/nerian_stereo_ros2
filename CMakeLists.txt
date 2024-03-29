cmake_minimum_required(VERSION 3.5)
project(nerian_stereo)

# Release-specific differences
set(LEGACY_ROS "galactic\|foxy\|eloquent\|dashing\|crystal\|bouncy\|ardent")
if(DEFINED ENV{ROS_DISTRO})
    if($ENV{ROS_DISTRO} MATCHES ${LEGACY_ROS})
        set(ROS2_IS_LEGACY 1)
    else()
        set(ROS2_IS_LEGACY 0)
    endif()
else()
    message(WARNING "ROS_VERSION is not set")
    if(${CMAKE_INSTALL_PREFIX} MATCHES ${LEGACY_ROS})
        set(ROS2_IS_LEGACY 1)
    else()
        set(ROS2_IS_LEGACY 0)
    endif()
endif()

if(ROS2_IS_LEGACY)
    message(STATUS "Building for legacy ROS")
else()
    message(STATUS "Building for ROS Humble or later")
endif()

# Default to C99
if(NOT CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 99)
endif()

# Default to C++14
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 14)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_action REQUIRED)
find_package(rosidl_default_generators)
find_package(std_msgs REQUIRED)
find_package(cv_bridge REQUIRED)
find_package(std_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(stereo_msgs REQUIRED)
find_package(tf2_ros REQUIRED)

find_package(OpenCV REQUIRED)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  # the following line skips the linter which checks for copyrights
  # uncomment the line when a copyright and license is not present in all source files
  #set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # uncomment the line when this package is not in a git repo
  #set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}/rosidl_generator_cpp
    ${CMAKE_CURRENT_LIST_DIR}/include
    ${OpenCV_INCLUDE_DIRS}
)

#######################
## libvisiontransfer ##
#######################

set(BUILD_VT 1)

if(BUILD_VT)
    # Build libvisiontransfer via git submodule (at specified branch)
    execute_process(COMMAND bash -c "cd ${CMAKE_CURRENT_SOURCE_DIR}; grep -v '#' visiontransfer_version.conf || echo default" OUTPUT_VARIABLE GIT_TAG_OR_BRANCH)
    execute_process(COMMAND bash -c "cd ${CMAKE_CURRENT_SOURCE_DIR}; git submodule update --init --recursive && git submodule foreach -q --recursive 'git fetch' && git submodule foreach -q --recursive 'git checkout -q ${GIT_TAG_OR_BRANCH}'")
    execute_process(COMMAND bash -c "mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/nerian-vision-software/libvisiontransfer")

    # Check for SSE4.1
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-msse -msse2 -msse3 -msse4.1" SSE4_1_SUPPORT)
    if(SSE4_1_SUPPORT)
        set(VT_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse2 -msse3 -msse4.1")
    else()
        set(VT_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    endif()

    # Build sources at runtime
    add_custom_target(nerian_stereo_visiontransfer_stub ALL
        COMMAND ${CMAKE_COMMAND} ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/libvisiontransfer -DCMAKE_CXX_FLAGS=${VT_CXX_FLAGS} -DDISABLE_NATIVE=1 -DDISABLE_PCL=1 -DDISABLE_OPENCV=1 -DDISABLE_PYTHON=1 -DDISABLE_OPEN3D=1
        COMMAND make
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/nerian-vision-software/libvisiontransfer
    )

    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/libvisiontransfer/lib/libvisiontransfer.so
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
    )

    install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/libvisiontransfer/visiontransfer
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include
        FILES_MATCHING PATTERN "*.h"
        PATTERN "CMakeFiles" EXCLUDE
    )

    include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/libvisiontransfer
        ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/nvcom/helpers
    )

    link_directories(${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/libvisiontransfer/lib)
    set(VISION_TRANSFER_LINKDIR ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/libvisiontransfer/lib)
    message(STATUS "Local libvisiontransfer link directory is: ${CMAKE_CURRENT_BINARY_DIR}/nerian-vision-software/libvisiontransfer/lib")

    set(COLORCODER_SOURCE_FILE ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/nerian-vision-software/nvcom/helpers/colorcoder.cpp)

else()
    # Development settings: Use local libvisiontransfer
    message(STATUS "Using local libvisiontransfer with LIB: $ENV{VISION_TRANSFER_LIB}   INCL: $ENV{VISION_TRANSFER_INCLUDE}")
    link_directories($ENV{VISION_TRANSFER_LIB})
    set(VISION_TRANSFER_LINKDIR $ENV{VISION_TRANSFER_LIB})
    include_directories($ENV{VISION_TRANSFER_INCLUDE})
    set(COLORCODER_SOURCE_FILE $ENV{VISION_TRANSFER_INCLUDE}/../libnvshared/nvshared/imageproc/colorcoder.cpp)
    include_directories($ENV{VISION_TRANSFER_INCLUDE}/../libnvshared/nvshared/imageproc)

    add_custom_target(nerian_stereo_visiontransfer_stub ALL
        COMMAND true
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endif()

#####################
##  Messages etc.  ##
#####################

rosidl_generate_interfaces(${PROJECT_NAME}
    "msg/StereoCameraInfo.msg"
    DEPENDENCIES std_msgs sensor_msgs stereo_msgs
)

#############
##  Build  ##
#############

add_executable(nerian_stereo_node
    src/nerian_stereo_node.cpp
    ${COLORCODER_SOURCE_FILE}
)
add_dependencies(nerian_stereo_node nerian_stereo_visiontransfer_stub)
ament_target_dependencies(nerian_stereo_node rclcpp tf2_ros cv_bridge)
if(${ROS2_IS_LEGACY} EQUAL 1)
    rosidl_target_interfaces(nerian_stereo_node ${PROJECT_NAME} "rosidl_typesupport_cpp")
else()
    rosidl_get_typesupport_target(cpp_typesupport_target ${PROJECT_NAME} "rosidl_typesupport_cpp")
    target_link_libraries(nerian_stereo_node "${cpp_typesupport_target}")
endif()
target_link_libraries(nerian_stereo_node ${OpenCV_LIBS} visiontransfer)
target_link_directories(nerian_stereo_node PRIVATE ${VISION_TRANSFER_LINKDIR})

install(TARGETS
  nerian_stereo_node
  DESTINATION lib/${PROJECT_NAME}
)
install(DIRECTORY
  launch
  DESTINATION share/${PROJECT_NAME}
)

ament_package()

