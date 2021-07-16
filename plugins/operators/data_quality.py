from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This operator performs checks against fact and dimensions table in Redshift database whether
    the table contains any rows and null values in specific columns.
    """
    
    ui_color = '#89DA59'

    row_count_table = """
        SELECT COUNT(*)
        FROM {table}
    """

    null_count_table = """
        SELECT COUNT(*)
        FROM {table}
        WHERE {table_pkey} IS NULL
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):
        """ Initialises the parameters of DataQualityOperator

        Args:
            redshift_conn_id (str): connection ID to a redshift database, specified in Airflow Admin Connection.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Raises:
            ValueError: If table returns no results, contains no rows or null value in specific column.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        """
        Checks if the table in Redshift database returns any results, contains any rows or null values in specific column.
        Raises ValueError otherwise.
        """
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        tables_dict = {'songs': 'song_id',
                        'artists': 'artist_id',
                        'users': 'user_id',
                        'time': '(start_time, hour, month, year, day, weekday)',
                        'songplays': 'songplay_id'}

        for table, table_pkey in tables_dict.items():
            self.log.info('DataQualityOperator is executing checks...')
            #### Check for row counts
            self.log.info(f'DataQualityOperator is checking row numbers in {table}')

            formatted_row_counts = DataQualityOperator.row_count_table.format(
                                    table=table
            )

            row_record = redshift.get_first(formatted_row_counts)

            if not row_record:
                raise ValueError(f'Test failed. {table} returned no results')
            else:
                if row_record[0] > 0:
                    self.log.info('Test passed.\nQuery:\n{}\nResults:\n{}\n----------'.format(
                        formatted_row_counts, row_record[0]
                    ))
                else:
                    raise ValueError(f"Test failed.\nQuery:\n{formatted_row_counts}\nResults:\n{row_record[0]}\n----------")

            #### Check for null values
            self.log.info(f'DataQualityOperator is checking for null values in {table}')
            
            formatted_null_counts = DataQualityOperator.null_count_table.format(
                                    table=table,
                                    table_pkey=table_pkey
            )

            null_record = redshift.get_first(formatted_null_counts)

            if not null_record:
                raise ValueError(f'Test failed. {table} returned no results')
            else:
                if null_record[0] == 0:
                    self.log.info('Test passed, {} does not contain null values.\nQuery:\n{}\nResults:\n{}\n----------'.format(
                        table, formatted_null_counts, null_record[0]
                    ))
                else:
                    raise ValueError(f"Test failed, {table} contains null values.\nQuery:\n{formatted_null_counts}\nResults:\n{null_record[0]}\n----------")
            self.log.info("Success.")
