import os
import unittest
from pathlib import Path

from DbMysql import DbMysql
from pydbzengine import Properties, BaseJsonChangeConsumer, DebeziumJsonEngineBuilder


class TestDebeziumJsonEngine(unittest.TestCase):
    OFFSET_FILE = Path(__file__).parent.joinpath('mysql-offsets.dat')

    def test_consuming(self):
        os.remove(self.OFFSET_FILE)
        SOURCEDB = DbMysql()
        SOURCEDB.start()
        # ///
        props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", self.OFFSET_FILE.as_posix())
        props.setProperty("offset.flush.interval.ms", "60000")
        # begin connector properties
        props.setProperty("database.hostname", SOURCEDB.CONTAINER.get_container_host_ip())
        props.setProperty("database.port", SOURCEDB.CONTAINER.get_exposed_port(SOURCEDB.MYSQL_PORT_DEFAULT))
        props.setProperty("database.user", SOURCEDB.MYSQL_DEBEZIUM_USER)
        props.setProperty("database.password", SOURCEDB.MYSQL_DEBEZIUM_PASSWORD)
        props.setProperty("database.server.id", "1234")
        props.setProperty("topic.prefix", "pydbz")
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
        props.setProperty("schema.history.internal.file.filename", "/path/to/storage/schemahistory.dat")
        # // debezium unwrap message
        props.setProperty("debezium.transforms", "unwrap")
        props.setProperty("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
        props.setProperty("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
        props.setProperty("debezium.transforms.unwrap.delete.handling.mode", "rewrite")
        props.setProperty("debezium.transforms.unwrap.drop.tombstones", "true")

        print("Creating engine")
        engine = DebeziumJsonEngineBuilder().with_properties(props).with_consumer(BaseJsonChangeConsumer()).build()
        print("Starting engine")
        engine.run()
        # engine.close()
