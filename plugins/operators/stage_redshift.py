from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
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
                 ingore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id=redshift_connection_id
        self.aws_credential_id=aws_credential_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers

    def execute(self, context):
#         self.log.info('StageToRedshiftOperator not implemented yet')
        





