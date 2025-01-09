/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Authors: Xie Han (xiehan@sogou-inc.com)
           Wu Jiaxu (wujiaxu@sogou-inc.com)
           Li Yingxin (liyingxin@sogou-inc.com)
*/

#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <time.h>
#include <netdb.h>
#include <stdio.h>
#include <string>
#include <functional>
#include <utility>
#include <atomic>
#include "WFGlobal.h"
#include "Workflow.h"
#include "WFTask.h"
#include "WFTaskError.h"

class __WFDynamicTask : public WFDynamicTask
{
protected:
	virtual void dispatch()
	{
		series_of(this)->push_front(this->create(this));
		this->WFDynamicTask::dispatch();
	}

protected:
	std::function<SubTask *(WFDynamicTask *)> create;

public:
	__WFDynamicTask(std::function<SubTask *(WFDynamicTask *)>&& create) :
		create(std::move(create))
	{
	}
};

inline WFDynamicTask *
WFTaskFactory::create_dynamic_task(dynamic_create_t create)
{
	return new __WFDynamicTask(std::move(create));
}

template<>
int WFTaskFactory::send_by_name(const std::string&, void *const *, size_t);

template<typename T>
int WFTaskFactory::send_by_name(const std::string& mailbox_name, T *const msg[],
								size_t max)
{
	return WFTaskFactory::send_by_name(mailbox_name, (void *const *)msg, max);
}

template<>
int WFTaskFactory::signal_by_name(const std::string&, void *const *, size_t);

template<typename T>
int WFTaskFactory::signal_by_name(const std::string& cond_name, T *const msg[],
								  size_t max)
{
	return WFTaskFactory::signal_by_name(cond_name, (void *const *)msg, max);
}

/************Go Task Factory************/

class __WFGoTask : public WFGoTask
{
public:
	void set_go_func(std::function<void ()> func)
	{
		this->go = std::move(func);
	}

protected:
	virtual void execute()
	{
		this->go();
	}

protected:
	std::function<void ()> go;

public:
	__WFGoTask(ExecQueue *queue, Executor *executor,
			   std::function<void ()>&& func) :
		WFGoTask(queue, executor),
		go(std::move(func))
	{
	}
};

class __WFTimedGoTask : public __WFGoTask
{
protected:
	virtual void dispatch();
	virtual SubTask *done();

protected:
	virtual void handle(int state, int error);

protected:
	static void timer_callback(WFTimerTask *timer);

protected:
	time_t seconds;
	long nanoseconds;
	std::atomic<int> ref;

public:
	__WFTimedGoTask(time_t seconds, long nanoseconds,
					ExecQueue *queue, Executor *executor,
					std::function<void ()>&& func) :
		__WFGoTask(queue, executor, std::move(func)),
		ref(4)
	{
		this->seconds = seconds;
		this->nanoseconds = nanoseconds;
	}
};

template<class FUNC, class... ARGS>
WFGoTask *WFTaskFactory::create_go_task(const std::string& queue_name,
										FUNC&& func, ARGS&&... args)
{
	auto&& tmp = std::bind(std::forward<FUNC>(func),
						   std::forward<ARGS>(args)...);
	return new __WFGoTask(WFGlobal::get_exec_queue(queue_name),
						  WFGlobal::get_compute_executor(),
						  std::move(tmp));
}

template<class FUNC, class... ARGS>
WFGoTask *WFTaskFactory::create_timedgo_task(time_t seconds, long nanoseconds,
											 const std::string& queue_name,
											 FUNC&& func, ARGS&&... args)
{
	auto&& tmp = std::bind(std::forward<FUNC>(func),
						   std::forward<ARGS>(args)...);
	return new __WFTimedGoTask(seconds, nanoseconds,
							   WFGlobal::get_exec_queue(queue_name),
							   WFGlobal::get_compute_executor(),
							   std::move(tmp));
}

template<class FUNC, class... ARGS>
WFGoTask *WFTaskFactory::create_go_task(ExecQueue *queue, Executor *executor,
										FUNC&& func, ARGS&&... args)
{
	auto&& tmp = std::bind(std::forward<FUNC>(func),
						   std::forward<ARGS>(args)...);
	return new __WFGoTask(queue, executor, std::move(tmp));
}

template<class FUNC, class... ARGS>
WFGoTask *WFTaskFactory::create_timedgo_task(time_t seconds, long nanoseconds,
											 ExecQueue *queue, Executor *executor,
											 FUNC&& func, ARGS&&... args)
{
	auto&& tmp = std::bind(std::forward<FUNC>(func),
						   std::forward<ARGS>(args)...);
	return new __WFTimedGoTask(seconds, nanoseconds,
							   queue, executor,
							   std::move(tmp));
}

template<class FUNC, class... ARGS>
void WFTaskFactory::reset_go_task(WFGoTask *task, FUNC&& func, ARGS&&... args)
{
	auto&& tmp = std::bind(std::forward<FUNC>(func),
						   std::forward<ARGS>(args)...);
	((__WFGoTask *)task)->set_go_func(std::move(tmp));
}

/**********Create go task with nullptr func**********/

template<> inline
WFGoTask *WFTaskFactory::create_go_task(const std::string& queue_name,
										std::nullptr_t&&)
{
	return new __WFGoTask(WFGlobal::get_exec_queue(queue_name),
						  WFGlobal::get_compute_executor(),
						  nullptr);
}

template<> inline
WFGoTask *WFTaskFactory::create_timedgo_task(time_t seconds, long nanoseconds,
											 const std::string& queue_name,
											 std::nullptr_t&&)
{
	return new __WFTimedGoTask(seconds, nanoseconds,
							   WFGlobal::get_exec_queue(queue_name),
							   WFGlobal::get_compute_executor(),
							   nullptr);
}

template<> inline
WFGoTask *WFTaskFactory::create_go_task(ExecQueue *queue, Executor *executor,
										std::nullptr_t&&)
{
	return new __WFGoTask(queue, executor, nullptr);
}

template<> inline
WFGoTask *WFTaskFactory::create_timedgo_task(time_t seconds, long nanoseconds,
											 ExecQueue *queue, Executor *executor,
											 std::nullptr_t&&)
{
	return new __WFTimedGoTask(seconds, nanoseconds, queue, executor, nullptr);
}

template<> inline
void WFTaskFactory::reset_go_task(WFGoTask *task, std::nullptr_t&&)
{
	((__WFGoTask *)task)->set_go_func(nullptr);
}

/**********Template Thread Task Factory**********/

template<class INPUT, class OUTPUT>
class __WFThreadTask : public WFThreadTask<INPUT, OUTPUT>
{
protected:
	virtual void execute()
	{
		this->routine(&this->input, &this->output);
	}

protected:
	std::function<void (INPUT *, OUTPUT *)> routine;

public:
	__WFThreadTask(ExecQueue *queue, Executor *executor,
				   std::function<void (INPUT *, OUTPUT *)>&& rt,
				   std::function<void (WFThreadTask<INPUT, OUTPUT> *)>&& cb) :
		WFThreadTask<INPUT, OUTPUT>(queue, executor, std::move(cb)),
		routine(std::move(rt))
	{
	}
};

template<class INPUT, class OUTPUT>
class __WFTimedThreadTask : public __WFThreadTask<INPUT, OUTPUT>
{
protected:
	virtual void dispatch();
	virtual SubTask *done();

protected:
	virtual void handle(int state, int error);

protected:
	static void timer_callback(WFTimerTask *timer);

protected:
	time_t seconds;
	long nanoseconds;
	std::atomic<int> ref;

public:
	__WFTimedThreadTask(time_t seconds, long nanoseconds,
						ExecQueue *queue, Executor *executor,
						std::function<void (INPUT *, OUTPUT *)>&& rt,
						std::function<void (WFThreadTask<INPUT, OUTPUT> *)>&& cb) :
		__WFThreadTask<INPUT, OUTPUT>(queue, executor, std::move(rt), std::move(cb)),
		ref(4)
	{
		this->seconds = seconds;
		this->nanoseconds = nanoseconds;
	}
};

template<class INPUT, class OUTPUT>
void __WFTimedThreadTask<INPUT, OUTPUT>::dispatch()
{
	WFTimerTask *timer;

	timer = WFTaskFactory::create_timer_task(this->seconds, this->nanoseconds,
											 __WFTimedThreadTask::timer_callback);
	timer->user_data = this;

	this->__WFThreadTask<INPUT, OUTPUT>::dispatch();
	timer->start();
}

template<class INPUT, class OUTPUT>
SubTask *__WFTimedThreadTask<INPUT, OUTPUT>::done()
{
	if (this->callback)
		this->callback(this);

	return series_of(this)->pop();
}

template<class INPUT, class OUTPUT>
void __WFTimedThreadTask<INPUT, OUTPUT>::handle(int state, int error)
{
	if (--this->ref == 3)
	{
		this->state = state;
		this->error = error;
		this->subtask_done();
	}

	if (--this->ref == 0)
		delete this;
}

template<class INPUT, class OUTPUT>
void __WFTimedThreadTask<INPUT, OUTPUT>::timer_callback(WFTimerTask *timer)
{
	auto *task = (__WFTimedThreadTask<INPUT, OUTPUT> *)timer->user_data;

	if (--task->ref == 3)
	{
		if (timer->get_state() == WFT_STATE_SUCCESS)
		{
			task->state = WFT_STATE_SYS_ERROR;
			task->error = ETIMEDOUT;
		}
		else
		{
			task->state = timer->get_state();
			task->error = timer->get_error();
		}

		task->subtask_done();
	}

	if (--task->ref == 0)
		delete task;
}

template<class INPUT, class OUTPUT>
WFThreadTask<INPUT, OUTPUT> *
WFThreadTaskFactory<INPUT, OUTPUT>::create_thread_task(const std::string& queue_name,
						std::function<void (INPUT *, OUTPUT *)> routine,
						std::function<void (WFThreadTask<INPUT, OUTPUT> *)> callback)
{
	return new __WFThreadTask<INPUT, OUTPUT>(WFGlobal::get_exec_queue(queue_name),
											 WFGlobal::get_compute_executor(),
											 std::move(routine),
											 std::move(callback));
}

template<class INPUT, class OUTPUT>
WFThreadTask<INPUT, OUTPUT> *
WFThreadTaskFactory<INPUT, OUTPUT>::create_thread_task(time_t seconds, long nanoseconds,
						const std::string& queue_name,
						std::function<void (INPUT *, OUTPUT *)> routine,
						std::function<void (WFThreadTask<INPUT, OUTPUT> *)> callback)
{
	return new __WFTimedThreadTask<INPUT, OUTPUT>(seconds, nanoseconds,
												  WFGlobal::get_exec_queue(queue_name),
												  WFGlobal::get_compute_executor(),
												  std::move(routine),
												  std::move(callback));
}

template<class INPUT, class OUTPUT>
WFThreadTask<INPUT, OUTPUT> *
WFThreadTaskFactory<INPUT, OUTPUT>::create_thread_task(ExecQueue *queue, Executor *executor,
						std::function<void (INPUT *, OUTPUT *)> routine,
						std::function<void (WFThreadTask<INPUT, OUTPUT> *)> callback)
{
	return new __WFThreadTask<INPUT, OUTPUT>(queue, executor,
											 std::move(routine),
											 std::move(callback));
}

template<class INPUT, class OUTPUT>
WFThreadTask<INPUT, OUTPUT> *
WFThreadTaskFactory<INPUT, OUTPUT>::create_thread_task(time_t seconds, long nanoseconds,
						ExecQueue *queue, Executor *executor,
						std::function<void (INPUT *, OUTPUT *)> routine,
						std::function<void (WFThreadTask<INPUT, OUTPUT> *)> callback)
{
	return new __WFTimedThreadTask<INPUT, OUTPUT>(seconds, nanoseconds,
												  queue, executor,
												  std::move(routine),
												  std::move(callback));
}

