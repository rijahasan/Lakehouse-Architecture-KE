from sqlalchemy.engine import default
import jaydebeapi
import jpype

class DremioDialect(default.DefaultDialect):
    def __init__(self, *args, **kwargs):
        super(DremioDialect, self).__init__(*args, **kwargs)
        self.driver = "org.apache.drill.jdbc.Driver"
        self.jdbc_url = "jdbc:dremio:direct={host}:{port}"

    def create_connect_args(self, url):
        host, port = url.host, url.port
        conn_args = [f"jdbc:dremio:direct://{host}:{port}/"]
        return conn_args, {}

    def is_disconnect(self, e, connection, cursor):
        return False

    def _get_jdbc_connection(self, url):
        return jaydebeapi.connect(
            self.driver, 
            self.jdbc_url.format(host=url.host, port=url.port), 
            {'user': url.username, 'password': url.password},
            jars="/app/superset_home/dremio-jdbc-driver-25.2.0-202410241428100111-a963b970.jar"
        )

    def get_jdbc_conn(self, url):
        return self._get_jdbc_connection(url)
