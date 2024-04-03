from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for test_sql in self.dq_checks:
            records = redshift_hook.get_records(test_sql['check_sql'])
            if len(records) < test_sql['expected_result'] or len(records[0]) < test_sql['expected_result']:
                raise ValueError(f"Data quality check failed. {test_sql['table']} less than {test_sql['expected_result']} rows")
            num_records = records[0][0]
            if num_records < test_sql['expected_result']:
                raise ValueError(f"Data quality check failed. {test_sql['table']} contained less than {test_sql['expected_result']} rows")
            logging.info(f"Data quality on table {test_sql['table']} check passed with {records[0][0]} records")