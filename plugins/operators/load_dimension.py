from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    This operator loads a dimension table with data from existing table(s) in the same Redshift database.
    Data is loaded either by appending new data to the existing dimension table or
    truncating existing dimension table before inserting data into it.
    It logs a message with level ERROR on the logger if load_type value entered is neither 'append' or 'truncate_insert'.
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_type="append",
                 redshift_schema="",
                 destination_table="",
                 sql_queries_table_insert=[],
                 *args, **kwargs):
        """ Initialises the parameters of LoadDimensionOperator

        Args:
            redshift_conn_id (str): connection ID to a redshift database, specified in Airflow Admin Connection.
            load_type (str): data loading pattern, defaulted to 'append'. Other option of the value is 'truncate_insert'.
            redshift_schema (str): schema in Redshift database that contains the tables (eg. 'public').
            destination_table (str): table name in Redshift schema to copy the data to.
            sql_queries_table_insert (list): SQL query of INSERT INTO TABLE statement.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_type=load_type.lower()
        self.redshift_schema=redshift_schema.lower()
        self.destination_table = destination_table.lower()
        self.sql_queries_table_insert=sql_queries_table_insert

    def execute(self, context):
        """
        Loads a dimension table by either appending new data to the existing dimension table or
        truncating existing dimension table before inserting data into it.
        Logs a message with level ERROR on the logger if load_type value entered is neither 'append' or 'truncate_insert'.
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_type.lower() not in ("append", "truncate_insert"):
            self.log.error("Please specify load_type. Options are 'append' and 'truncate_insert'.")

        if self.load_type.lower() == 'append':
            self.log.info(f'LoadDimensionOperator is appending data into {redshift_schema}.{destination_table}...')
            redshift.run(self.sql_queries_table_insert)
            self.log.info(f'LoadDimensionOperator has completed loading data into {self.redshift_schema}.{self.destination_table}')
        elif self.load_type.lower() == 'truncate_insert':
            self.log.info(f'LoadDimensionOperator is loading data with "truncate_insert" load type into {self.redshift_schema}.{self.destination_table}...')
            
            formatted_truncate_table = SqlQueries.truncate_table.format(
                                            redshift_schema=self.redshift_schema,
                                            destination_table=self.destination_table
            )
            
            redshift.run([formatted_truncate_table, self.sql_queries_table_insert])
            
            self.log.info(f'LoadDimensionOperator has completed loading data into {self.redshift_schema}.{self.destination_table}.')

        self.log.info("Success.")
