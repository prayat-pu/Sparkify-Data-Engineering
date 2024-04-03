from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql='',
                 operation='append-only',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.operation = operation

    def execute(self, context):
        self.log.info('Connecting to AWS and Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.operation == 'delete-load':
            self.log.info("Clearing data from destination Redshift table")
            redshift.run(f"delete from {self.table}")

        # run sql query
        self.log.info('Loading Dimension data from staging area')
        redshift.run(self.sql)
