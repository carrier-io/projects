import json
from traceback import format_exc
from typing import Optional, Tuple, List
from flask import request, g
from pylon.core.tools import log

from pydantic import ValidationError
from ...models.statistics_test import StatisticTest

from tools import auth, db, api_tools, db_tools



class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.view"],
        "recommended_roles": {
            "default": {"admin": True, "viewer": True, "editor": True},
            "developer": {"admin": True, "viewer": True, "editor": True},
        }})
    def get(self, project_id: int | None = None) -> tuple[dict, int] | tuple[list, int]:
        return None


class AdminAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.view"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": False},
        }})
    def get(self, project_id: int | None = None) -> tuple[dict, int] | tuple[list, int]:

        query_result = StatisticTest.query.all()
        statisticts = []
        for report in query_result:
            statisticts.append(
                {
                    'id': report.id,
                    'project_id': report.project_id,
                    'test_type': report.test_type,
                    'end_time': str(report.end_time),
                    'cpu_usage': report.cpu * report.duration * report.runners,
                    'memory_usage': report.memory * report.duration * report.runners,
                     'is_project_resourses': report.is_project_resourses
                }
            )

        return {'total': len(statisticts), 'rows': statisticts}, 200


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
