from typing import Union
import redis
import json

from ..models.project import Project
from ..models.quota import ProjectQuota
from ..models.statistics import Statistic
from ..models.statistics_test import StatisticTest

from tools import rpc_tools
from pylon.core.tools import web, log

from ..tools.session_project import SessionProject
from tools import constants


class RPC:
    @web.rpc('project_get_or_404', 'get_or_404')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def prj_or_404(self, project_id):
        return Project.get_or_404(project_id)

    @web.rpc('project_list', 'list')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def list_projects(self, **kwargs):
        return Project.list_projects(**kwargs)

    @web.rpc('project_statistics', 'statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_project_statistics(self, project_id):
        return Statistic.query.filter_by(project_id=project_id).first().to_json()

    @web.rpc('projects_add_task_execution', 'add_task_execution')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def add_task_execution(self, project_id):
        try:
            statistic = Statistic.query.filter_by(project_id=project_id).first()
            setattr(statistic, 'tasks_executions', Statistic.tasks_executions + 1)
            statistic.commit()
        except AttributeError:
            ...

    @web.rpc('project_get_storage_space_quota', 'get_storage_space_quota')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_storage_quota(self, project_id):
        return Project.get_storage_space_quota(project_id=project_id)

    @web.rpc('project_check_quota', 'check_quota')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def check_quota(self, project_id, quota=None):
        return ProjectQuota.check_quota_json(project_id, quota)

    @web.rpc('project_get_id', 'get_id')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_id(self):
        project_id = SessionProject.get()
        # if not project_id:
        #     project_id = get_user_projects()[0]["id"]
        return project_id

    @web.rpc('project_set_active', 'set_active')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def set_active(self, project_id: Union[str, int]):
        SessionProject.set(int(project_id))

    @web.rpc('increment_statistics', 'increment_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def increment_statistics(self, project_id, column: str, amount: int = 1):
        statistic = Statistic.query.filter_by(project_id=project_id).first()
        setattr(statistic, column, getattr(statistic, column) + amount)
        statistic.commit()

    # @web.rpc('update_rabbit_queues', 'update_rabbit_queues')
    # @rpc_tools.wrap_exceptions(RuntimeError)
    # def update_rabbit_queues(self, vhost, queues):
    #
    #     return f"Project queues updated"

    @web.rpc('register_rabbit_queue', 'register_rabbit_queue')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def register_rabbit_queue(self, vhost, queue_name):
        _rc = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=4,
                          password=constants.REDIS_PASSWORD, username=constants.REDIS_USER)
        queues = _rc.get(name=vhost)
        queues = json.loads(queues) if queues else []
        if queue_name not in queues:
            queues.append(queue_name)
            _rc.set(name=vhost, value=json.dumps(queues))
            return f"Queue with name {queue_name} registered"
        return f"Queue with name {queue_name} already exist"

    @web.rpc('get_rabbit_queues', 'get_rabbit_queues')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_rabbit_queues(self, vhost: str, remove_internal: bool = False) -> list:
        _rc = redis.Redis(
            host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=4,
            password=constants.REDIS_PASSWORD, username=constants.REDIS_USER
        )
        try:
            # log.info('get_rabbit_queues vhost: [%s], RC.get %s', vhost, _rc.get(name=vhost))
            raw = _rc.get(name=vhost)
            log.info('get_rabbit_queues vhost: [%s], queues: [%s]', vhost, raw)
            queues = json.loads(raw)
        except TypeError:
            return []
        if remove_internal:
            try:
                queues.remove('__internal')
            except ValueError:
                ...
        return queues

    @web.rpc('update_test_statistics', 'update_test_statistics')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def update_test_statistics(self, report_data: dict, test_type: str):
        cloud_settings = report_data['test_config']['env_vars']['cloud_settings']
        is_project_resourses = False
        if cloud_settings:
            is_project_resourses = True
            integration_name = cloud_settings['integration_name']
            integration = self.context.rpc_manager.call.integrations_get_admin_defaults(integration_name)
            if integration.id == cloud_settings['id'] and not cloud_settings['project_id']:
                is_project_resourses = False

        statistic_test = StatisticTest(
            project_id = report_data['project_id'],
            test_type = test_type,
            test_uid = report_data['test_uid'],
            report_uid = report_data['uid'],
            start_time = report_data['start_time'],
            end_time = report_data['end_time'],
            duration = report_data['duration'],
            cpu = report_data['test_config']['env_vars']['cpu_quota'],
            memory = report_data['test_config']['env_vars']['memory_quota'],
            runners = report_data['test_config']['parallel_runners'],
            is_project_resourses = is_project_resourses
        )
        statistic_test.insert()
