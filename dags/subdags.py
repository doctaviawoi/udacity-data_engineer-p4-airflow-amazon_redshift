from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import StageToRedshiftOperator
from airflow.operators.udacity_plugin import LoadDimensionOperator

from helpers import SqlQueries

import sql


def load_dim_table_subdag(
        parent_dag_name,
        child_dag_name,
        redshift_conn_id,
        load_type,
        redshift_schema,
        default_args,
        *args, **kwargs):
        """
        This function generates a DAG to be used as a subDAG that runs LoadDimensionOperator on
        dimension tables in Redshift database, namely 'songs', 'artists', 'users' and 'time'.

        Args:
            parent_dag_name (str): name specified for the parent DAG.
            child_dag_name (str): task name specified for the subdag.
            redshift_conn_id (str): connection ID to a redshift database, specified in Airflow Admin Connection.
            load_type (str): data loading pattern, defaulted to 'append'. Other option of the value is 'truncate_insert'.
            redshift_schema (str): schema in Redshift database that contains the tables (eg. 'public').
            default_args (dict): set of arguments for task created. They can be overriden during operator initialisation.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            dag_subdag (airflow.models.DAG): DAG to use as a subdag.
        """
        
        dag_subdag = DAG(
                         f"{parent_dag_name}.{child_dag_name}",
                         schedule_interval='@hourly',
                         start_date=default_args['start_date']
        )

        sql_stmt_dic = {'songs': SqlQueries.songs_table_insert,
                        'artists': SqlQueries.artists_table_insert,
                        'users': SqlQueries.users_table_insert,
                        'time': SqlQueries.time_table_insert
        }

        for destination_table, sql_stmt in sql_stmt_dic.items():
            LoadDimensionOperator(
                task_id=f"insert_data_{destination_table}_dim_table",
                dag=dag_subdag,
                redshift_conn_id=redshift_conn_id,
                load_type="truncate_insert",
                redshift_schema=redshift_schema,
                destination_table=destination_table,
                sql_queries_table_insert=sql_stmt
            )

        return dag_subdag
