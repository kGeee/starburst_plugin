import sys
sys.path.append('../')
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import logging
from starburst_plugin.hooks.trino import TrinoHook
from airflow.hooks.S3_hook import S3Hook
from typing import Optional, Union, List
import boto3
import uuid

class Starburst_S3Operator(BaseOperator):
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
    template_fields = ['sql','s3_location']
    template_fields_renderers = {'sql': 'sql'}
    template_ext = ['.sql']
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql: str,
            s3_location,
            autocommit: bool = True,
            parameters: Optional[dict] = None,
            *args,
            **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.s3_location = s3_location
        self.autocommit = autocommit
        self.parameters = parameters
        self.hook = None
        self.bucket = None
        self.prefix = None

    def getFile(self):
        import os
        s3 = S3Hook(aws_conn_id='aws_default')
        s3.get_conn()
        print(self.bucket, self.prefix)
        keys = s3.list_keys(
                    bucket_name = self.bucket,
                    prefix =  self.prefix )

        path = Variable.get("QUERY_DESTINATION")

        for k in keys:
            dest = os.path.join(path, k)
            if not os.path.exists(os.path.dirname(dest)):
                os.makedirs(os.path.dirname(dest))
            
            p = "/".join(dest.split('/')[:-1])
            s3.download_file(key=k, bucket_name = self.bucket, local_path=p)


    def convert_types(self, sql, context):
        import re
        try:
            reg = re.search('(select) (.+?) (from.+)', sql)
            variables = reg.group(2)
            cast_variables = list()
            for var in variables.split(","):
                split_string = [x for x in var.split(' ') if x]  
                var_to_cast = split_string[0]
                split_string[0] = f"cast({var_to_cast} as varchar)"
                cast_variables.append(" ".join(split_string))

            pre = reg.group(1)
            post = reg.group(3)
            uid = str(uuid.uuid4())

            
            print("here", self.s3_location)
            s3_parse = re.search("(s3[s]?:\/\/)?(.*)/(.*/.*)", self.s3_location)
            context['ti'].xcom_push(key='bucket', value=s3_parse.group(2))
            context['ti'].xcom_push(key='prefix', value=s3_parse.group(3)+ '-' + uid)
            context['ti'].xcom_push(key='uid', value=uid)
            

            self.bucket = s3_parse.group(2)
            self.prefix = s3_parse.group(3)+ '-' + uid

            cast_sql = pre + " " + ",".join(cast_variables) + " " + post
            self.sql = f"create table hive.default.temp with (external_location='{self.s3_location}-{uid}', format='CSV') as " + cast_sql 

        except AttributeError:
            pass
            
        

    def execute(self, context):
        """
        Execute query against Starburst Enterprise Platform
        """
        logging.info(f"Running SQL :{self.sql}")
        self.hook = TrinoHook()
        self.convert_types(self.sql, context)
        print(self.sql)
        query = self.hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)

        self.getFile()
        

        return self.s3_location



