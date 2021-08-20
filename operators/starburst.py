from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from starburst_plugin.hooks.trino import TrinoHook
from typing import Optional, Union, List

class StarburstOperator(BaseOperator):
    """
    Executes sql code in a Starburst cluster

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param starburst_conn_id: reference to specific trino connection id
    :type starburst_conn_id: str
    :param schema: schema to run queries on; overloads connection provided schema
    """
    template_fields = ['sql']
    template_fields_renderers = {'sql': 'sql'}
    template_ext = ['.sql']
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql: Union[str, List[str]],
            autocommit: bool = True,
            parameters: Optional[dict] = None,
            xcom_push: bool = True,
            *args,
            **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.xcom_push = xcom_push
        self.hook = None

    def execute(self, context):
        """
        Execute query against Starburst Enterprise Platform
        """
        logging.info(f"Running SQL :{self.sql}")
        self.hook = TrinoHook()
        query = self.hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)
        if self.xcom_push:
            return query



