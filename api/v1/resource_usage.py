import json
from datetime import datetime
from typing import Optional, Tuple, List
from flask import request
from pylon.core.tools import log

from tools import auth, api_tools



class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.view"],
        "recommended_roles": {
            "default": {"admin": True, "viewer": True, "editor": True},
            "developer": {"admin": True, "viewer": True, "editor": True},
        }})
    def get(self, project_id: int | None = None) -> tuple[dict, int] | tuple[list, int]:
        resource_type = request.args.get('type').lower()
        if start_time := request.args.get('start_time'):
            start_time = datetime.fromisoformat(start_time.strip('Z'))
        if end_time := request.args.get('end_time'):
            end_time = datetime.fromisoformat(end_time.strip('Z'))
        if resource_type == 'tests':
            statisticts = self.module.get_test_statistics(project_id, start_time, end_time)
        if resource_type == 'tasks':
            statisticts = self.module.get_task_statistics(project_id, start_time, end_time)
        return {'total': len(statisticts), 'rows': statisticts}, 200


class AdminAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.view"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": False},
        }})
    def get(self, project_id: int | None = None) -> tuple[dict, int] | tuple[list, int]:
        resource_type = request.args.get('type').lower()
        if start_time := request.args.get('start_time'):
            start_time = datetime.fromisoformat(start_time.strip('Z'))
        if end_time := request.args.get('end_time'):
            end_time = datetime.fromisoformat(end_time.strip('Z'))
        if resource_type == 'tests':
            statisticts = self.module.get_test_statistics(project_id, start_time, end_time)
        if resource_type == 'tasks':
            statisticts = self.module.get_task_statistics(project_id, start_time, end_time)
        return {'total': len(statisticts), 'rows': statisticts}, 200

    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.view"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": False},
        }})
    def put(self, project_id: int | None = None) -> tuple[dict, int] | tuple[list, int]:
        data = request.json
        self.module.update_test_statistics(data)


class API(api_tools.APIBase):  # pylint: disable=R0903
    url_params = [
        "",
        "<string:mode>",
        "<string:mode>/<int:project_id>",
    ]

    mode_handlers = {
        'administration': AdminAPI,
        'default': ProjectAPI,
    }
