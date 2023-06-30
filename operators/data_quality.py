from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self, redshift_conn, tables_list,*args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn
        self.tables = tables_list
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Start checking data quality")
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if not records or not records[0]:
                raise ValueError(f"Data quality check failed. {table} returned no results") 
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
        self.log.info("Completed checking data quality")