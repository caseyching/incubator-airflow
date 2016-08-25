from airflow.hooks.postgres_hook import PostgresHook


class RedshiftHook(PostgresHook):

    conn_name_attr = 'redshift_conn_id'
    default_conn_name = 'redshift_default'

    def get_conn(self):
        self.postgres_conn_id = self.redshift_conn_id
        conn = super(RedshiftHook, self).get_conn()

        # Override auto-commit support. This is set during parent get_conn()
        # call.
        self.supports_autocommit = True

        return conn

