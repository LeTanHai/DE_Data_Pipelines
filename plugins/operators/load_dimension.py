from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                postgres_conn_id='',
                table_name='',
                sql='',
                truncate=False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        if self.truncate:
            self.log.info(f'Truncate table {self.table} before loading dimention data')
            postgres.run(f'TRUNCATE {self.table}')

        self.log.info(f'Load demention table {self.table}')
        postgres.run(f'INSERT INTO {self.table} {self.sql}')
