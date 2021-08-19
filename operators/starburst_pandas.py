from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from starburst_plugin.hooks.trino import TrinoHook

class StarburstPandasOperator(BaseOperator):
    """
    Executes sql code in a Starburst cluster

    :param starburst_conn_id: reference to specific trino connection id
    :type starburst_conn_id: str
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param schema: schema to run queries on; overloads connection provided schema
    """
    template_fields = ['sql']
    template_ext = ['.sql']

    @apply_defaults
    def __init__(
            self,
            sql,
            starburst_conn_id: str = 'trino',
            *args,
            **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.starburst_conn_id = starburst_conn_id

    def get_hook(self) -> TrinoHook:
        """
        Create and return TrinoHook
        :return: TrinoHook instance
        """
        return TrinoHook()

    def execute(self, context) -> None:
        """
        Execute query against Starburst Enterprise Platform
        :return: None
        """
        logging.info(f"Running SQL :{self.sql}")
        hook = self.get_hook()
        df = hook.get_pandas_df(self.sql)
        return df




