from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    """
    This operator connects to a Redshift cluster using a specified AWS credentials in Airflow Connection, then
    execute a TRUNCATE TABLE command on an existing staging table in Redshift and followed by
    a COPY command on an JSON file from S3 to a staging table in Redshift.
    """
    
    ui_color = '#358140'

    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 region="",
                 redshift_schema="",
                 destination_table="",
                 s3_bucket="",
                 s3_key="",
                 format_as_json='auto',
                 *args, **kwargs):
        
        """ Initialises the parameters of StageToRedshiftOperator

        Args:
            redshift_conn_id (str): connection ID to a redshift database, specified in Airflow Admin Connection.
            aws_credentials_id (str): AWS credentials details specified in Airflow Admin Connection.
            region (str): AWS region where the source data is located (eg. 'us-west-2' or 'ap-southeast-2').
            redshift_schema (str): schema in Redshift database that contains the tables (eg. 'public').
            destination_table (str): table name in Redshift schema to copy the data to.
            s3_bucket (str): S3 bucket to copy the data from.
            s3_key (str): object path in a specific S3 bucket.
            format_as_json (str): mapping option of the JSON-formatted source data, value defaulted to 'auto'.
                Other value options are 'auto ignorecase', 's3://jsonpaths_file', 'noshred'.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region=region.lower()
        self.redshift_schema=redshift_schema.lower()
        self.destination_table = destination_table.lower()
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format_as_json=format_as_json

    def execute(self, context):
        """
        Connects to Redshift cluster using the AWS credentials specified in Airflow Connection then
        truncates all rows in the destination staging table in Redshift database before
        copying data from S3 object path to the staging table.
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        #s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        self.log.info(f"Copying data from {s3_path} to Redshift staging table {self.redshift_schema}.{self.destination_table}...")
        
        formatted_truncate_table = SqlQueries.truncate_table.format(
                                redshift_schema=self.redshift_schema,
                                destination_table=self.destination_table
        )
        
        formatted_copy_table = SqlQueries.copy_table.format(
                                destination_table=self.destination_table,
                                s3_path=s3_path,
                                aws_access_key=credentials.access_key,
                                aws_secret_key=credentials.secret_key,
                                region=self.region,
                                json_format=self.format_as_json
        )
        
        redshift.run([formatted_truncate_table, formatted_copy_table])

        self.log.info(f"StageToRedshiftOperator has completed loading data from {s3_path} to Redshift staging table {self.redshift_schema}.{self.destination_table}")

        self.log.info("Success.")
