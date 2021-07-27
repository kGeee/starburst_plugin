from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults
import sys
sys.path.append('../')
from starburst_plugin.hooks.trino_hook import TrinoHook

class TrinoToS3Operator(BaseOperator, SkipMixin):

    template_fields = ['name']
    template_ext = ['.csv']

    @apply_defaults
    def __init__(
            self,
            name: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.hook = None

    def execute(self, context):
        message = "Hello {}".format(self.name)
        print(message)
        if not self.hook:
            self.hook = TrinoHook()
            x = self.hook.get_connection()
            print(self.hook)
        return message