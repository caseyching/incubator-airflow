import logging

from airflow.contrib.hooks.redshift_hook import RedshiftHook
from airflow.operators import PostgresOperator
from airflow.utils.decorators import apply_defaults


class RedshiftOperator(PostgresOperator):

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self, redshift_conn_id='redshift_default', *args, **kwargs):
        super(RedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        self.hook = RedshiftHook(redshift_conn_id=self.redshift_conn_id)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
