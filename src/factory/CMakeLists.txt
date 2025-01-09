cmake_minimum_required(VERSION 3.6)
project(factory)

set(SRC
	WFGraphTask.cc
	WFTaskFactory.cc
	Workflow.cc
	WFResourcePool.cc
	WFMessageQueue.cc
	FileTaskImpl.cc
)

add_library(${PROJECT_NAME} OBJECT ${SRC})
