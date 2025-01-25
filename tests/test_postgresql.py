import os
import unittest
from pathlib import Path

from DbPostgresql import DbPostgresql
from pydbzengine import Properties, DebeziumJsonEngineBuilder, BaseJsonChangeConsumer


class TestDebeziumJsonEngine(unittest.TestCase):
    OFFSET_FILE = Path(__file__).parent.joinpath('postgresql-offsets.dat')

    def get_props(self, SOURCEDB):
        if self.OFFSET_FILE.exists():
            os.remove(self.OFFSET_FILE)
        props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("database.hostname", SOURCEDB.CONTAINER.get_container_host_ip())
        props.setProperty("database.port", SOURCEDB.CONTAINER.get_exposed_port(SOURCEDB.POSTGRES_PORT_DEFAULT))
        props.setProperty("database.user", SOURCEDB.POSTGRES_USER)
        props.setProperty("database.password", SOURCEDB.POSTGRES_PASSWORD)
        props.setProperty("database.dbname", SOURCEDB.POSTGRES_DBNAME)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", self.OFFSET_FILE.as_posix())
        props.setProperty("max.batch.size", "1255")
        props.setProperty("poll.interval.ms", "10000")
        # props.setProperty("debezium.format.value.schemas.enable", "true")
        props.setProperty("offset.flush.interval.ms", "1000")
        props.setProperty("database.server.name", "testc")
        props.setProperty("database.server.id", "1234")
        props.setProperty("topic.prefix", "testc")
        props.setProperty("schema.whitelist", "inventory")
        props.setProperty("database.whitelist", "inventory")
        props.setProperty("table.whitelist", "inventory.*")
        props.setProperty("replica.identity.autoset.values", "inventory.*:FULL")
        # // debezium unwrap message
        props.setProperty("debezium.transforms", "unwrap")
        props.setProperty("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
        props.setProperty("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
        props.setProperty("debezium.transforms.unwrap.delete.handling.mode", "rewrite")
        props.setProperty("debezium.transforms.unwrap.drop.tombstones", "true")
        return props

    def test_consuming(self):
        SOURCEDB = DbPostgresql()
        SOURCEDB.start()
        props = self.get_props(SOURCEDB)
        print("Creating engine")
        engine = DebeziumJsonEngineBuilder().with_properties(props).with_consumer(BaseJsonChangeConsumer()).build()
        print("Starting engine")
        engine.run()
