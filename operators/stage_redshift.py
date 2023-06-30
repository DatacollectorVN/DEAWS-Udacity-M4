from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    @apply_defaults
    def __init__(self,
                redshift_conn,
                aws_credentials,
                table,
                s3_bucket,
                s3_key,
                log_json_file="",
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.aws_credentials_id = aws_credentials
        self.execution_date = kwargs.get('execution_date')
        

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Start clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Complete clearing data from destination Redshift table")

        self.log.info("Start copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.log_json_file != "":
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            json_mode = self.log_json_file
        else:
            json_mode = 'auto'

        formatted_sql = """
            COPY {} 
            FROM '{}' 
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS json '{}';
        """.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            json_mode 
        )
        redshift.run(formatted_sql)
        self.log.info(f"Complete copying data to Redshift table {self.table}")