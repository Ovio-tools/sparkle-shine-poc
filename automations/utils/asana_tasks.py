"""
automations/utils/asana_tasks.py

Helper for creating Asana tasks in a named project section.
"""
from typing import Optional
import asana
from asana.rest import ApiException as AsanaApiException


def create_tasks(
    client: asana.ApiClient,
    project_name: str,
    section_name: str,
    tasks: list,
    tool_ids: dict,
    deduplicate_by_title: bool = False,
) -> list:
    """
    Create Asana tasks inside a specific project section.

    Parameters
    ----------
    client          : asana.ApiClient from get_client("asana")
    project_name    : human name, e.g. "Client Success"
    section_name    : human name, e.g. "Onboarding"
    tasks           : list of dicts:
                        { title, description (opt), assignee_email (opt),
                          due_date (ISO string, opt) }
    tool_ids        : the full tool_ids dict loaded from config/tool_ids.json
    deduplicate_by_title : if True, skip tasks whose title already exists in
                           the project

    Returns
    -------
    list of GID strings for the tasks that were actually created
    """
    tasks_api = asana.TasksApi(client)
    sections_api = asana.SectionsApi(client)

    # Resolve project GID
    project_gid = tool_ids["asana"]["projects"].get(project_name)
    if not project_gid:
        raise ValueError(
            f"Project '{project_name}' not found in tool_ids['asana']['projects']"
        )

    # Resolve section GID
    section_gid = (
        tool_ids["asana"]["sections"]
        .get(project_name, {})
        .get(section_name)
    )
    if not section_gid:
        raise ValueError(
            f"Section '{section_name}' not found in "
            f"tool_ids['asana']['sections']['{project_name}']"
        )

    # Build existing-title set if deduplication is requested
    existing_titles: set = set()
    if deduplicate_by_title:
        opts = {"project": project_gid, "opt_fields": "name"}
        for task in tasks_api.get_tasks_for_project(project_gid, opts):
            existing_titles.add(task["name"])

    created_gids = []

    for task_def in tasks:
        title = task_def["title"]

        if deduplicate_by_title and title in existing_titles:
            continue

        body: dict = {
            "data": {
                "name": title,
                "projects": [project_gid],
            }
        }

        if task_def.get("description"):
            body["data"]["notes"] = task_def["description"]

        if task_def.get("due_date"):
            body["data"]["due_on"] = task_def["due_date"]

        if task_def.get("assignee_email"):
            body["data"]["assignee"] = task_def["assignee_email"]

        try:
            created = tasks_api.create_task(body, {})
        except AsanaApiException as exc:
            # 400 often means the assignee is not a member of this workspace.
            # Retry without the assignee so the task is still created.
            if exc.status == 400 and body["data"].get("assignee"):
                print(
                    f"[WARN] Asana task '{title}': assignee "
                    f"'{body['data']['assignee']}' rejected (400) — "
                    f"retrying without assignee"
                )
                body["data"].pop("assignee")
                created = tasks_api.create_task(body, {})
            else:
                raise
        task_gid = created["gid"]

        # Move to the target section
        sections_api.add_task_for_section(
            section_gid,
            {"body": {"data": {"task": task_gid}}},
        )

        created_gids.append(task_gid)

    return created_gids
