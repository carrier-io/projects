import json
from traceback import format_exc
from typing import Optional, Tuple, List
from flask import request, g
from pylon.core.tools import log

from pydantic import ValidationError

from tools import auth, VaultClient, TaskManager, db, api_tools, db_tools

from sqlalchemy.exc import NoResultFound
from ...models.pd.project import ProjectCreatePD
from ...models.project import Project

from ...utils import get_project_user
from ...utils.project_steps import ProjectModel, ProjectSchema, SystemUser, SystemToken, \
    ProjectSecrets, \
    InfluxDatabases, RabbitVhost, get_steps, ProjectPermissions, Invitations


class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.view"],
        "recommended_roles": {
            "default": {"admin": True, "viewer": True, "editor": True},
            "developer": {"admin": True, "viewer": True, "editor": True},
        }})
    def get(self, project_id: int | None = None) -> tuple[dict, int] | tuple[list, int]:
        log.info('g.auth.id %s', g.auth.id)
        if g.auth.id is None:
            return list(), 200
        #
        offset_ = request.args.get("offset")
        limit_ = request.args.get("limit")
        search_ = request.args.get("search")
        #
        return self.module.list_user_projects(
            g.auth.id, offset_=offset_, limit_=limit_, search_=search_
        ), 200


class AdminAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.view"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": False},
        }})
    def get(self, **kwargs) -> tuple[dict, int] | tuple[list, int]:
        log.info('g.auth.id %s', g.auth.id)
        if g.auth.id is None:
            return list(), 200
        #
        offset_ = request.args.get("offset")
        limit_ = request.args.get("limit")
        search_ = request.args.get("search")
        #
        return self.module.list_user_projects(
            g.auth.id, offset_=offset_, limit_=limit_, search_=search_
        ), 200

    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.create"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": False},
            "default": {"admin": False, "viewer": False, "editor": False},
            "developer": {"admin": False, "viewer": False, "editor": False},
        }})
    def post(self, **kwargs) -> tuple[dict, int]:
        # Validate incoming data
        status_code = 201
        try:
            project_model = ProjectCreatePD.parse_obj(request.json)
        except ValidationError as e:
            return e.errors(), 400

        steps = list(get_steps(self.module))

        try:

            # Create project model
            project = ProjectModel().create(project_model, g.auth.id)

            # Create project schema
            ProjectSchema().create(project.id)

            # Get permissions and roles
            ProjectPermissions(self.module).create(project.id)

            # Create system user and token
            system_user_id = SystemUser().create(project.id)
            system_token = SystemToken().create(system_user_id)

            # Create project secrets
            vault_client = ProjectSecrets().create(project, system_token)

            # Init project databases
            RabbitVhost().create(vault_client)
            # InfluxDatabases().create(vault_client)

            # self.module.context.rpc_manager.timeout(3).check_rabbit_queues()
            # log.info('after run rabbit task')
            # self.module.context.rpc_manager.call.populate_backend_runners_table(project.id)

            # create project admin
            log.info('adding project admin')
            ROLES = ['admin', ]
            self.module.add_user_to_project_or_create(
                # user_name=project_model.project_admin_email,
                user_email=project_model.project_admin_email,
                project_id=project.id,
                roles=ROLES
            )

            # Send invitations here
            Invitations(self.module).create(project_model)

            # create default s3 integration
            # self.module.context.rpc_manager.timeout(3).integrations_create_default_s3_for_new_project(project.id)
            self.module.context.event_manager.fire_event('project_created', project.to_json())

        except Exception as e:
            log.critical(format_exc())
            status_code = 400
        statuses: List[dict] = [step.status['created'] for step in steps]
        return {'steps': statuses}, status_code

    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.edit"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": False},
            "default": {"admin": False, "viewer": False, "editor": False},
            "developer": {"admin": False, "viewer": False, "editor": False},
        }})
    def put(self, project_id: Optional[int] = None) -> Tuple[dict, int]:
        # data = self._parser_post.parse_args()
        data = request.json
        if not project_id:
            return {"message": "Specify project id"}, 400
        project = Project.get_or_404(project_id)
        if data["name"]:
            project.name = data["name"]
        if data["owner"]:
            project.owner = data["owner"]
        if data["plugins"]:
            project.plugins = data["plugins"]
        project.commit()
        return project.to_json(exclude_fields=Project.API_EXCLUDE_FIELDS), 200

    @auth.decorators.check_api({
        "permissions": ["projects.projects.project.delete"],
        "recommended_roles": {
            "administration": {"admin": True, "viewer": False, "editor": False},
            "default": {"admin": False, "viewer": False, "editor": False},
            "developer": {"admin": False, "viewer": False, "editor": False},
        }})
    def delete(self, project_id: int):
        project = Project.get_or_404(project_id)
        try:
            system_user_id = get_project_user(project.id)['id']
        except (RuntimeError, KeyError, NoResultFound):
            system_user_id = None

        context = {
            'project': project,
            'project_id': project.id,
            'vault_client': VaultClient.from_project(project),
            'system_user_id': system_user_id
        }

        # log.info('DELETE %s', steps)
        statuses: List[dict] = []
        steps = list(get_steps(self.module))
        for step in reversed(steps):
            try:
                step.delete(**context)
            except Exception as e:
                log.warning('%s error %s', repr(step), e)
            statuses.append(step.status['deleted'])

        return {'steps': statuses}, 200


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
