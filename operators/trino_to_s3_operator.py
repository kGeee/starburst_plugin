from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, SkipMixin

from hooks.trino_hook import TrinoHook

class TrinoToS3Operator(BaseOperator, SkipMixin):
    pass