from airflow.plugins_manager import AirflowPlugin
from starburst_plugin.operators.starburst import StarburstOperator
from starburst_plugin.operators.starburst_s3 import Starburst_S3Operator
from starburst_plugin.hooks.trino import TrinoHook

class StarburstPlugin(AirflowPlugin):
    name = "starburst_plugin"
    operators = [StarburstOperator, Starburst_S3Operator]
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
