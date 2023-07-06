from typing import Union, Optional
import json
from datetime import datetime

from ..models.resource_usage import ResourceUsage
from ..models.resource_usage_tasks import ResourceUsageTasks

from tools import rpc_tools
from pylon.core.tools import web, log

from tools import constants


class RPC:
    @web.rpc('get_test_statistics', 'get_test_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_test_statistics(
            self, project_id: int | None = None,
            start_time: datetime | None = None,
            end_time: datetime | None = None
            ):
        statisticts = []
        if project_id:
            query = ResourceUsage.query.filter(
                ResourceUsage.project_id == project_id
                )
        else:
            query = ResourceUsage.query
        if start_time:
            query = query.filter(ResourceUsage.start_time >= start_time.isoformat())
        if end_time:
            query = query.filter(ResourceUsage.start_time <= end_time.isoformat())
        query_results = query.all()
        for result in query_results:
            statisticts.append(
                {
                    'id': result.id,
                    'project_id': result.project_id,
                    'test_type': result.test_type,
                    'start_time': str(result.start_time),
                    'cpu_usage': result.cpu * result.duration,
                    'memory_usage': result.memory * result.duration,
                    'is_project_resourses': result.is_project_resourses
                }
            )
        return statisticts

    @web.rpc('create_test_statistics', 'create_test_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def create_test_statistics(self, report_data: dict, test_type: str):
        cloud_settings = report_data['test_config']['env_vars']['cloud_settings']
        is_project_resourses = False
        if cloud_settings:
            is_project_resourses = True
            integration_name = cloud_settings['integration_name']
            integration = self.context.rpc_manager.call.integrations_get_admin_defaults(integration_name)
            if integration and integration.id == cloud_settings['id'] and not cloud_settings['project_id']:
                is_project_resourses = False

        statistic_test = ResourceUsage(
            project_id = report_data['project_id'],
            test_type = test_type,
            test_uid = report_data['test_uid'],
            report_uid = report_data['uid'],
            report_id = report_data['id'],            
            start_time = report_data['start_time'],
            # end_time = report_data['end_time'],
            cpu = report_data['test_config']['env_vars']['cpu_quota'],
            memory = report_data['test_config']['env_vars']['memory_quota'],
            runners = report_data['test_config']['parallel_runners'],
            location = report_data['test_config']['location'],
            is_cloud = bool(cloud_settings),
            is_project_resourses = is_project_resourses
        )
        statistic_test.insert()

    @web.rpc('update_test_statistics', 'update_test_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def update_test_statistics(self, report_data: dict):
        report_id = report_data.pop('report_id')
        resources = ResourceUsage.query.filter(ResourceUsage.report_id == report_id).first()
        resource_usage = list(resources.resource_usage)
        resource_usage.append(report_data)
        resources.resource_usage = resource_usage
        resources.duration += report_data['time_to_sleep']
        resources.commit()

    @web.rpc('get_task_statistics', 'get_task_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_task_statistics(            
            self, project_id: int | None = None,
            start_time: datetime | None = None,
            end_time: datetime | None = None
            ):
        statisticts = []
        if project_id:
            query = ResourceUsageTasks.query.filter(
                ResourceUsageTasks.project_id == project_id
                )
        else:
            query = ResourceUsageTasks.query
        if start_time:
            query = query.filter(ResourceUsageTasks.start_time >= start_time.isoformat())
        if end_time:
            query = query.filter(ResourceUsageTasks.start_time <= end_time.isoformat())
        query_results = query.all()
        for result in query_results:
            statisticts.append(
                {
                    'id': result.id,
                    'project_id': result.project_id,
                    'task_name': result.task_name,
                    'start_time': str(result.start_time),
                    'cpu_usage': result.cpu * result.duration,
                    'memory_usage': result.memory * result.duration,
                    'location': result.location,
                    'is_project_resourses': result.is_project_resourses
                }
            )
        return statisticts

    @web.rpc('create_task_statistics', 'create_task_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def create_task_statistics(self, task_data: dict):
        is_cloud = False  # TODO: must change it when we will be able to run tasks in clouds
        is_project_resourses = False
        statistic_task = ResourceUsageTasks(
            project_id = task_data['project_id'],
            task_id = task_data['id'],
            task_name = task_data['task_name'],
            task_result_id = task_data['task_result_id'],
            test_report_id = task_data.get('test_report_id'),
            start_time = task_data['start_time'],
            cpu = json.loads(task_data['env_vars']).get('cpu_cores', 1),
            memory = json.loads(task_data['env_vars']).get('memory', 1),
            runners = json.loads(task_data['env_vars']).get('runners', 1),
            is_cloud = is_cloud,
            location = task_data['region'],
            is_project_resourses = is_project_resourses
        )
        statistic_task.insert()

    @web.rpc('update_task_statistics', 'update_task_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def update_task_statistics(self, task_data: dict):
        statistic_task = ResourceUsageTasks.query.filter(
            ResourceUsageTasks.task_result_id == task_data['id']
            ).first()
        statistic_task.duration = round(task_data['task_duration'])
        resource_usage = {
            'time': str(datetime.now()),          
        }
        if task_data['task_stats'] and 'kubernetes_stats' in task_data['task_stats']:
            resource_usage.update({
                'cpu_limit': int(task_data['task_stats']['kubernetes_stats'][0]['cpu_limit']),
                'memory_limit': task_data['task_stats']['kubernetes_stats'][0]['memory_limit']
            })
        elif task_data['task_stats']:
            resource_usage.update({
            'cpu': round(float(task_data['task_stats']["cpu_stats"]["cpu_usage"]["total_usage"]) / 1000000000, 2),
            'memory_usage': round(float(task_data['task_stats']["memory_stats"]["usage"]) / (1024 * 1024), 2),
            'memory_limit': round(float(task_data['task_stats']["memory_stats"]["limit"]) / (1024 * 1024), 2),     
            })

        statistic_task.resource_usage = resource_usage
        statistic_task.commit()
