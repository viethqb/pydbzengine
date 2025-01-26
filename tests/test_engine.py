import unittest

from pydbzengine import Properties, BasePythonChangeHandler, DebeziumJsonEngine


class TestDebeziumJsonEngine(unittest.TestCase):

    def test_wrong_config_raises_error(self):
        props = Properties()
        props.setProperty("name", "my-connector")
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("transforms", "router")
        props.setProperty("transforms.router.type", "org.apache.kafka.connect.transforms.NotExists")

        with self.assertRaisesRegex(Exception,
                                    ".*Error.*while.*instantiating.*transformation.*router"):  # Wrong message
            DebeziumJsonEngine(properties=props, handler=BasePythonChangeHandler())

        # test engine arguments validated
        with self.assertRaisesRegex(Exception,
                                    ".*Please provide debezium config.*"):  # Wrong message
            DebeziumJsonEngine(properties=None, handler=BasePythonChangeHandler())
        with self.assertRaisesRegex(Exception,
                                    ".*Please provide handler.*"):  # Wrong message
            DebeziumJsonEngine(properties=props, handler=None)
