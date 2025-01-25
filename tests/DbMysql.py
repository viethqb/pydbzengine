from testcontainers.core.config import testcontainers_config
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.mysql import MySqlContainer


def _connect(self) -> None:
    wait_for_logs(self, ".*mysqld: ready for connections.*")


class DbMysql:
    MYSQL_ROOT_PASSWORD = "debezium"
    MYSQL_USER = "mysqluser"
    MYSQL_PASSWORD = "mysqlpw"
    MYSQL_DEBEZIUM_USER = "debezium"
    MYSQL_DEBEZIUM_PASSWORD = "dbz"
    MYSQL_IMAGE = "debezium/example-mysql:3.0.0.Final"
    MYSQL_HOST = "127.0.0.1"
    MYSQL_DATABASE = "inventory"
    MYSQL_PORT_DEFAULT = 3306

    CONTAINER: MySqlContainer = (MySqlContainer(image=MYSQL_IMAGE,
                                                username=MYSQL_USER,
                                                root_password=MYSQL_ROOT_PASSWORD,
                                                password=MYSQL_PASSWORD,
                                                # dbname=MYSQL_DATABASE,
                                                port=MYSQL_PORT_DEFAULT
                                                )
                                 .with_exposed_ports(MYSQL_PORT_DEFAULT)
                                 )

    MySqlContainer._connect = _connect

    def start(self):
        testcontainers_config.ryuk_disabled = True
        self.CONTAINER.start()

    def stop(self):
        self.CONTAINER.stop()

    def __exit__(self, exc_type, exc_value, traceback):
        self.CONTAINER.stop()
