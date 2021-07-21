from airflow.plugins_manager import AirflowPlugin
from operators.trino_to_s3_operator import TrinoToS3Operator
from hooks.trino_hook import TrinoHook


class StarburstPlugin(AirflowPlugin):
    name = "starburst_plugin"
    operators = [TrinoToS3Operator]
    hooks = [TrinoHook]