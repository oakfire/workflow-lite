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
*/

#ifndef _WFGLOBAL_H_
#define _WFGLOBAL_H_

#if __cplusplus < 201100
#error CPLUSPLUS VERSION required at least C++11. Please use "-std=c++11".
#include <C++11_REQUIRED>
#endif

#include <string>
#include "CommScheduler.h"
#include "Executor.h"
#include "WFResourcePool.h"

/**
 * @file    WFGlobal.h
 * @brief   Workflow Global Settings & Workflow Global APIs
 */

/**
 * @brief   Workflow Library Global Setting
 * @details
 * If you want set different settings with default, please call WORKFLOW_library_init at the beginning of the process
*/
struct WFGlobalSettings
{
	unsigned int dns_ttl_default;	///< in seconds, DNS TTL when network request success
	unsigned int dns_ttl_min;		///< in seconds, DNS TTL when network request fail
	int dns_threads;
	int poller_threads;
	int handler_threads;
	int compute_threads;			///< auto-set by system CPU number if value<0
	int fio_max_events;
	const char *resolv_conf_path;
	const char *hosts_path;
};

/**
 * @brief   Default Workflow Library Global Settings
 */
static constexpr struct WFGlobalSettings GLOBAL_SETTINGS_DEFAULT =
{
	.dns_ttl_default	=	3600,
	.dns_ttl_min		=	60,
	.dns_threads		=	4,
	.poller_threads		=	4,
	.handler_threads	=	20,
	.compute_threads	=	-1,
	.fio_max_events		=	4096,
	.resolv_conf_path	=	"/etc/resolv.conf",
	.hosts_path			=	"/etc/hosts",
};

/**
 * @brief      Reset Workflow Library Global Setting
 * @param[in]  settings          custom settings pointer
*/
extern void WORKFLOW_library_init(const struct WFGlobalSettings *settings);

/**
 * @brief   Workflow Global Management Class
 * @details Workflow Global APIs
 */
class WFGlobal
{
public:

	/**
	 * @brief      get current global settings
	 * @return     current global settings const pointer
	 * @note       returnval never NULL
	 */
	static const struct WFGlobalSettings *get_global_settings()
	{
		return &settings_;
	}

	static void set_global_settings(const struct WFGlobalSettings *settings)
	{
		settings_ = *settings;
	}

	static const char *get_error_string(int state, int error);

	static bool increase_handler_thread()
	{
		return WFGlobal::get_scheduler()->increase_handler_thread() == 0;
	}

	static bool decrease_handler_thread()
	{
		return WFGlobal::get_scheduler()->decrease_handler_thread() == 0;
	}

	static bool increase_compute_thread()
	{
		return WFGlobal::get_compute_executor()->increase_thread() == 0;
	}

	static bool decrease_compute_thread()
	{
		return WFGlobal::get_compute_executor()->decrease_thread() == 0;
	}

	// Internal usage only
public:
	static bool is_scheduler_created();
	static class CommScheduler *get_scheduler();
	static class ExecQueue *get_exec_queue(const std::string& queue_name);
	static class Executor *get_compute_executor();
	static class IOService *get_io_service();

public:
	static int sync_operation_begin();
	static void sync_operation_end(int cookie);

private:
	static struct WFGlobalSettings settings_;

};

#endif

