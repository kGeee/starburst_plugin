from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import sys
sys.path.append('../')
from starburst_plugin.hooks.trino_hook import TrinoHook

class StarburstOperator(BaseOperator):
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
            starburst_conn_id: str = 'trino',
            sql: str = 'show catalogs;',
            *args,
            **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.starburst_conn_id = starburst_conn_id
        self.hook = None

    def execute(self, context): 
        print("Running:{}".format(self.sql))     
        if not self.hook:
            self.hook = TrinoHook()
            connection = self.hook.get_connection(self.starburst_conn_id)
            self.hook.get_records(self.sql)
