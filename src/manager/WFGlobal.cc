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

  Authors: Wu Jiaxu (wujiaxu@sogou-inc.com)
           Xie Han (xiehan@sogou-inc.com)
           Liu Kai (liukaidx@sogou-inc.com)
*/

#include <arpa/inet.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <stdio.h>
#include <ctype.h>
#include <string>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "CommScheduler.h"
#include "Executor.h"
#include "WFResourcePool.h"
#include "WFTaskError.h"
#include "WFGlobal.h"

class __WFGlobal
{
public:
	static __WFGlobal *get_instance()
	{
		static __WFGlobal kInstance;
		return &kInstance;
	}

	void sync_operation_begin()
	{
		bool inc;

		sync_mutex_.lock();
		inc = ++sync_count_ > sync_max_;
		if (inc)
			sync_max_ = sync_count_;

		sync_mutex_.unlock();
		if (inc)
			WFGlobal::increase_handler_thread();
	}

	void sync_operation_end()
	{
		int dec = 0;

		sync_mutex_.lock();
		if (--sync_count_ < (sync_max_ + 1) / 2)
		{
			dec = sync_max_ - 2 * sync_count_;
			sync_max_ -= dec;
		}

		sync_mutex_.unlock();
		while (dec > 0)
		{
			WFGlobal::decrease_handler_thread();
			dec--;
		}
	}

private:
	__WFGlobal();

private:
	std::mutex sync_mutex_;
	int sync_count_;
	int sync_max_;
};

__WFGlobal::__WFGlobal()
{
	sync_count_ = 0;
	sync_max_ = 0;
}

class __FileIOService : public IOService
{
public:
	__FileIOService(CommScheduler *scheduler):
		scheduler_(scheduler),
		flag_(true)
	{}

	int bind()
	{
		mutex_.lock();
		flag_ = false;

		int ret = scheduler_->io_bind(this);

		if (ret < 0)
			flag_ = true;

		mutex_.unlock();
		return ret;
	}

	void deinit()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		while (!flag_)
			cond_.wait(lock);

		lock.unlock();
		IOService::deinit();
	}

private:
	virtual void handle_unbound()
	{
		mutex_.lock();
		flag_ = true;
		cond_.notify_one();
		mutex_.unlock();
	}

	virtual void handle_stop(int error)
	{
		scheduler_->io_unbind(this);
	}

	CommScheduler *scheduler_;
	std::mutex mutex_;
	std::condition_variable cond_;
	bool flag_;
};

class __CommManager
{
public:
	static __CommManager *get_instance()
	{
		static __CommManager kInstance;
		__CommManager::created_ = true;
		return &kInstance;
	}

	CommScheduler *get_scheduler() { return &scheduler_; }
	IOService *get_io_service();
	static bool is_created() { return created_; }

private:
	__CommManager():
		fio_service_(NULL),
		fio_flag_(false)
	{
		const auto *settings = WFGlobal::get_global_settings();
		if (scheduler_.init(settings->poller_threads,
							settings->handler_threads) < 0)
			abort();

		signal(SIGPIPE, SIG_IGN);
	}

	~__CommManager()
	{
		// scheduler_.deinit() will triger fio_service to stop
		scheduler_.deinit();
		if (fio_service_)
		{
			fio_service_->deinit();
			delete fio_service_;
		}
	}

private:
	CommScheduler scheduler_;
	__FileIOService *fio_service_;
	volatile bool fio_flag_;
	std::mutex fio_mutex_;

private:
	static bool created_;
};

bool __CommManager::created_ = false;

inline IOService *__CommManager::get_io_service()
{
	if (!fio_flag_)
	{
		fio_mutex_.lock();
		if (!fio_flag_)
		{
			int maxevents = WFGlobal::get_global_settings()->fio_max_events;
			int n = 65536;

			fio_service_ = new __FileIOService(&scheduler_);
			while (fio_service_->init(maxevents) < 0)
			{
				if ((errno != EAGAIN && errno != EINVAL) || maxevents <= 16)
					abort();

				while (n >= maxevents)
					n /= 2;

				maxevents = n;
			}

			if (fio_service_->bind() < 0)
				abort();

			fio_flag_ = true;
		}

		fio_mutex_.unlock();
	}

	return fio_service_;
}

class __ExecManager
{
protected:
	using ExecQueueMap = std::unordered_map<std::string, ExecQueue *>;

public:
	static __ExecManager *get_instance()
	{
		static __ExecManager kInstance;
		return &kInstance;
	}

	ExecQueue *get_exec_queue(const std::string& queue_name);
	Executor *get_compute_executor() { return &compute_executor_; }

private:
	__ExecManager():
		rwlock_(PTHREAD_RWLOCK_INITIALIZER)
	{
		int compute_threads = WFGlobal::get_global_settings()->compute_threads;

		if (compute_threads < 0)
			compute_threads = sysconf(_SC_NPROCESSORS_ONLN);

		if (compute_executor_.init(compute_threads) < 0)
			abort();
	}

	~__ExecManager()
	{
		compute_executor_.deinit();

		for (auto& kv : queue_map_)
		{
			kv.second->deinit();
			delete kv.second;
		}

		pthread_rwlock_destroy(&rwlock_);
	}

private:
	pthread_rwlock_t rwlock_;
	ExecQueueMap queue_map_;
	Executor compute_executor_;
};

inline ExecQueue *__ExecManager::get_exec_queue(const std::string& queue_name)
{
	ExecQueue *queue = NULL;
	ExecQueueMap::const_iterator iter;

	pthread_rwlock_rdlock(&rwlock_);
	iter = queue_map_.find(queue_name);
	if (iter != queue_map_.cend())
		queue = iter->second;

	pthread_rwlock_unlock(&rwlock_);
	if (queue)
		return queue;

	pthread_rwlock_wrlock(&rwlock_);
	iter = queue_map_.find(queue_name);
	if (iter == queue_map_.cend())
	{
		queue = new ExecQueue();
		if (queue->init() >= 0)
			queue_map_.emplace(queue_name, queue);
		else
		{
			delete queue;
			queue = NULL;
		}
	}
	else
		queue = iter->second;

	pthread_rwlock_unlock(&rwlock_);
	return queue;
}

struct WFGlobalSettings WFGlobal::settings_ = GLOBAL_SETTINGS_DEFAULT;

bool WFGlobal::is_scheduler_created()
{
	return __CommManager::is_created();
}

CommScheduler *WFGlobal::get_scheduler()
{
	return __CommManager::get_instance()->get_scheduler();
}

ExecQueue *WFGlobal::get_exec_queue(const std::string& queue_name)
{
	return __ExecManager::get_instance()->get_exec_queue(queue_name);
}

Executor *WFGlobal::get_compute_executor()
{
	return __ExecManager::get_instance()->get_compute_executor();
}

IOService *WFGlobal::get_io_service()
{
	return __CommManager::get_instance()->get_io_service();
}

int WFGlobal::sync_operation_begin()
{
	if (WFGlobal::is_scheduler_created() &&
		WFGlobal::get_scheduler()->is_handler_thread())
	{
		__WFGlobal::get_instance()->sync_operation_begin();
		return 1;
	}

	return 0;
}

void WFGlobal::sync_operation_end(int cookie)
{
	if (cookie)
		__WFGlobal::get_instance()->sync_operation_end();
}

static inline const char *__get_task_error_string(int error)
{
	switch (error)
	{
	case WFT_ERR_URI_PARSE_FAILED:
		return "URI Parse Failed";

	case WFT_ERR_URI_SCHEME_INVALID:
		return "URI Scheme Invalid";

	case WFT_ERR_URI_PORT_INVALID:
		return "URI Port Invalid";

	case WFT_ERR_UPSTREAM_UNAVAILABLE:
		return "Upstream Unavailable";

	case WFT_ERR_HTTP_BAD_REDIRECT_HEADER:
		return "Http Bad Redirect Header";

	case WFT_ERR_HTTP_PROXY_CONNECT_FAILED:
		return "Http Proxy Connect Failed";

	case WFT_ERR_REDIS_ACCESS_DENIED:
		return "Redis Access Denied";

	case WFT_ERR_REDIS_COMMAND_DISALLOWED:
		return "Redis Command Disallowed";

	case WFT_ERR_MYSQL_HOST_NOT_ALLOWED:
		return "MySQL Host Not Allowed";

	case WFT_ERR_MYSQL_ACCESS_DENIED:
		return "MySQL Access Denied";

	case WFT_ERR_MYSQL_INVALID_CHARACTER_SET:
		return "MySQL Invalid Character Set";

	case WFT_ERR_MYSQL_COMMAND_DISALLOWED:
		return "MySQL Command Disallowed";

	case WFT_ERR_MYSQL_QUERY_NOT_SET:
		return "MySQL Query Not Set";

	case WFT_ERR_MYSQL_SSL_NOT_SUPPORTED:
		return "MySQL SSL Not Supported";

	case WFT_ERR_KAFKA_PARSE_RESPONSE_FAILED:
		return "Kafka parse response failed";

	case WFT_ERR_KAFKA_PRODUCE_FAILED:
		return "Kafka produce api failed";

	case WFT_ERR_KAFKA_FETCH_FAILED:
		return "Kafka fetch api failed";

	case WFT_ERR_KAFKA_CGROUP_FAILED:
		return "Kafka cgroup failed";

	case WFT_ERR_KAFKA_COMMIT_FAILED:
		return "Kafka commit api failed";

	case WFT_ERR_KAFKA_META_FAILED:
		return "Kafka meta api failed";

	case WFT_ERR_KAFKA_LEAVEGROUP_FAILED:
		return "Kafka leavegroup failed";

	case WFT_ERR_KAFKA_API_UNKNOWN:
		return "Kafka api type unknown";

	case WFT_ERR_KAFKA_VERSION_DISALLOWED:
		return "Kafka broker version not supported";

    case WFT_ERR_KAFKA_SASL_DISALLOWED:
        return "Kafka sasl disallowed";

    case WFT_ERR_KAFKA_ARRANGE_FAILED:
        return "Kafka arrange failed";

    case WFT_ERR_KAFKA_LIST_OFFSETS_FAILED:
        return "Kafka list offsets failed";

    case WFT_ERR_KAFKA_CGROUP_ASSIGN_FAILED:
        return "Kafka cgroup assign failed";

	case WFT_ERR_CONSUL_API_UNKNOWN:
		return "Consul api type unknown";

	case WFT_ERR_CONSUL_CHECK_RESPONSE_FAILED:
		return "Consul check response failed";

	default:
		break;
	}

	return "Unknown";
}

const char *WFGlobal::get_error_string(int state, int error)
{
	switch (state)
	{
	case WFT_STATE_SUCCESS:
		return "Success";

	case WFT_STATE_TOREPLY:
		return "To Reply";

	case WFT_STATE_NOREPLY:
		return "No Reply";

	case WFT_STATE_SYS_ERROR:
		return strerror(error);

	case WFT_STATE_TASK_ERROR:
		return __get_task_error_string(error);

	case WFT_STATE_ABORTED:
		return "Aborted";

	case WFT_STATE_UNDEFINED:
		return "Undefined";

	default:
		break;
	}

	return "Unknown";
}

void WORKFLOW_library_init(const struct WFGlobalSettings *settings)
{
	WFGlobal::set_global_settings(settings);
}

