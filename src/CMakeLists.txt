cmake_minimum_required(VERSION 3.6)

include_directories(${INC_DIR}/workflow)
include_directories(kernel)

add_subdirectory(kernel)
add_subdirectory(manager)
add_subdirectory(factory)

add_dependencies(kernel LINK_HEADERS)
add_dependencies(manager LINK_HEADERS)
add_dependencies(factory LINK_HEADERS)

set(STATIC_LIB_NAME ${PROJECT_NAME}-static)
set(SHARED_LIB_NAME ${PROJECT_NAME}-shared)

add_library(
	${STATIC_LIB_NAME} STATIC
	$<TARGET_OBJECTS:kernel>
	$<TARGET_OBJECTS:manager>
	$<TARGET_OBJECTS:factory>
)

add_library(
	${SHARED_LIB_NAME} SHARED
	$<TARGET_OBJECTS:kernel>
	$<TARGET_OBJECTS:manager>
	$<TARGET_OBJECTS:factory>
)



if(ANDROID)
	target_link_libraries(${SHARED_LIB_NAME} c)
	target_link_libraries(${STATIC_LIB_NAME} c)
else()
	target_link_libraries(${SHARED_LIB_NAME} pthread)
	target_link_libraries(${STATIC_LIB_NAME} pthread)
endif ()

set_target_properties(${STATIC_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})
set_target_properties(${SHARED_LIB_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_NAME} VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

install(
	TARGETS ${STATIC_LIB_NAME}
	ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
	COMPONENT devel
)

install(
	TARGETS ${SHARED_LIB_NAME}
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
	COMPONENT devel
)

target_include_directories(${STATIC_LIB_NAME} BEFORE PUBLIC
	"$<BUILD_INTERFACE:${INC_DIR}>"
	"$<INSTALL_INTERFACE:${INC_DIR}>")
target_include_directories(${SHARED_LIB_NAME} BEFORE PUBLIC
	"$<BUILD_INTERFACE:${INC_DIR}>"
	"$<INSTALL_INTERFACE:${INC_DIR}>")

