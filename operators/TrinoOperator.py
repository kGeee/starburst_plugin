from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import sys
sys.path.append('../')
from starburst_plugin.hooks.trino_hook import TrinoHook

class TrinoOperator(BaseOperator):

    template_fields = ['sql']
    template_ext = ['.sql']

    @apply_defaults
    def __init__(
            self,
            sql: str,
            conn_id: str = 'trino_default',
            *args,
            **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.hook = None

    def execute(self, context):      
        if not self.hook:
            self.hook = TrinoHook()
            print("Running:{}".format(self.sql))
            self.hook.run(self.sql)

