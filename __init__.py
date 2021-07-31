from airflow.plugins_manager import AirflowPlugin
from starburst_plugin.operators.TrinoOperator import TrinoOperator
from starburst_plugin.hooks.trino_hook import TrinoHook


class StarburstPlugin(AirflowPlugin):
    name = "starburst_plugin"
    operators = [TrinoOperator]
    hooks = [TrinoHook]
    sensors = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []