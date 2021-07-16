from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    This operator loads a fact table with data from a staging data in the same Redshift database.
    Data is loaded either by appending new data to the existing fact table or
    truncating existing fact table before inserting data into it.
    It logs a message with level ERROR on the logger if load_type value entered is neither 'append' or 'truncate_insert'.
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_type="append",
                 redshift_schema="",
                 destination_table="",
                 *args, **kwargs):
        """ Initialises the parameters of LoadFactOperator

        Args:
            redshift_conn_id (str): connection ID to a redshift database, specified in Airflow Admin Connection.
            load_type (str): data loading pattern, defaulted to 'append'. Other option of the value is 'truncate_insert'.
            redshift_schema (str): schema in Redshift database that contains the tables (eg. 'public').
            destination_table (str): table name in Redshift schema to copy the data to.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_type=load_type.lower()
        self.redshift_schema=redshift_schema.lower()
        self.destination_table = destination_table.lower()

    def execute(self, context):
        """
        Loads a fact table by either appending new data to the existing fact table or
        truncating existing fact table before inserting data into it.
        Logs a message with level ERROR on the logger if load_type value entered is neither 'append' or 'truncate_insert'.
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_type.lower() not in ("append", "truncate_insert"):
            self.log.error("Please specify load_type. Options are 'append' and 'truncate_insert'.")

        if self.load_type.lower() == 'append':
            self.log.info(f'LoadFactOperator is appending data into {self.redshift_schema}.{self.destination_table}...')
            redshift.run(SqlQueries.songplay_table_insert)
            self.log.info(f'LoadFactOperator has completed loading data to {self.redshift_schema}.{self.destination_table}.')
        elif self.load_type.lower() == 'truncate_insert':
            self.log.info(f'LoadFactOperator is loading data with "truncate_insert" load type into {self.redshift_schema}.{self.destination_table}...')
            
            formatted_truncate_table = SqlQueries.truncate_table.format(
                                        redshift_schema=self.redshift_schema,
                                        destination_table=self.destination_table
            )
            
            redshift.run([formatted_truncate_table, SqlQueries.songplay_table_insert])
            
            self.log.info(f'LoadFactOperator has completed loading data into {self.redshift_schema}.{self.destination_table}.')

        self.log.info("Success.")
