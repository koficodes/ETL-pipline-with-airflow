from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id=""
                 aws_credential_id="",
                 sql_query="",
                 delimiter=","
                 ignore_headers=1,                
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id=redshift_connection_id
        self.aws_credential_id=aws_credential_id
        self.sql_query=sql_query
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers

    def execute(self, context):
        credentials  = AwsHook(self.aws_credential_id).get_credentials()
        redshift = PostgresHook(postgres_conn_id=redshift_connection_id)
        
        formatted_sql = LoadFactOperator.sql.format(
            self.sql_query,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        
        redshift.run(formatted_sql)