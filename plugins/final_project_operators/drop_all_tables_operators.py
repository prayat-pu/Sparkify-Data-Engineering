from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DropALLTablesRedShift(BaseOperator):
    drop_sql = """drop table if exists {}"""
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list=[],
                 *args, **kwargs):

        super(DropALLTablesRedShift, self).__init__(*args, **kwargs)
        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id
        
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Delete table from destination Redshift table")
        for table in self.table_list:
            formatted_sql = DropALLTablesRedShift.drop_sql.format(table)
            redshift.run(formatted_sql)
