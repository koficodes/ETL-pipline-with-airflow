from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    sql = """
        INSERT INTO {}
        {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_connection_id="",
                 aws_credential_id="",
                 sql_query="",
                 delimiter=",",
                 ignore_headers=1,
                 delete_load=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id=redshift_connection_id
        self.aws_credential_id=aws_credential_id
        self.sql_query=sql_query
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers
        self.delete_load=delete_load

    def execute(self, context):
        credentials  = AwsHook(self.aws_credential_id).get_credentials()
        redshift = PostgresHook(postgres_conn_id=redshift_connection_id)
        
        if self.delete_load:
            self.log.info(f"Clearing data from {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
            
            
        formatted_sql = LoadFactOperator.sql.format(
            self.sql_query,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        
        self.log.info(f"Running {formatted_sql}")
        redshift.run(formatted_sql)