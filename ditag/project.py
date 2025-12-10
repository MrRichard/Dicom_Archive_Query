import os
import yaml
from . import config

PROJECTS_DIR = os.path.join(config.APP_DIR, 'projects')

def get_project_dir(project_name):
    """Get the directory for a specific project."""
    return os.path.join(PROJECTS_DIR, project_name)

def get_project_config_path(project_name):
    """Get the path to the project's config file."""
    return os.path.join(get_project_dir(project_name), f"{project_name}.yml")

def get_project_db_path(project_name):
    """Get the path to the project's database file."""
    return os.path.join(get_project_dir(project_name), f"{project_name}.db")

def get_project_config(project_name):
    """Read a project's config file."""
    config_path = get_project_config_path(project_name)
    if not os.path.exists(config_path):
        return None
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def save_project_config(project_name, proj_config):
    """Save a project's config file."""
    config_path = get_project_config_path(project_name)
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, 'w') as f:
        yaml.dump(proj_config, f)

def create_project(project_name, pacs_ip, pacs_port, pacs_aetitle, target_list):
    """Create a new project and its configuration."""
    project_dir = get_project_dir(project_name)
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)

    proj_config = {
        'project_name': project_name,
        'pacs': {
            'host': pacs_ip,
            'port': pacs_port,
            'aetitle': pacs_aetitle
        },
        'database_path': get_project_db_path(project_name),
        'target_list': target_list,
        'cost_per_study': None
    }
    save_project_config(project_name, proj_config)
    return proj_config
