import unittest

from pydbzengine import Properties, BaseJsonChangeConsumer, DebeziumJsonEngineBuilder


class TestDebeziumJsonEngine(unittest.TestCase):

    def test_wrong_config_raises_error(self):
        props = Properties()
        props.setProperty("name", "my-connector")
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("transforms", "router")
        props.setProperty("transforms.router.type", "org.apache.kafka.connect.transforms.NotExists")

        with self.assertRaisesRegex(Exception,
                                    ".*Error.*while.*instantiating.*transformation.*router"):  # Wrong message
            DebeziumJsonEngineBuilder().with_properties(props).with_consumer(BaseJsonChangeConsumer()).build()
