from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_connection_id="",
                 aws_credential_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 delete_load=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id=redshift_connection_id
        self.aws_credential_id=aws_credential_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers
        self.delete_load=delete_load

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        
        if self.delete_load:
           self.log.info(f"Clearing data from {self.table}")
           redshift.run("DELETE FROM {}".format(self.table))
        
        formatted_key = self.s3_key.format(**context)
        s3_path = "s3a://{}/{}".format(self.s3_bucket, formatted_key)
        
        formatted_sql = StageToRedshiftOperator.sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        
        self.log.info(f"Running {formatted_sql}")
        redshift.run(formatted_sql)





