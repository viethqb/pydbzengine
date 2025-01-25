from abc import ABC
from pathlib import Path
from typing import List

################# INIT ####################
DEBEZIUM_JAVA_LIBS_DR = Path(__file__).parent.joinpath("debezium/libs/*").as_posix()
import jnius_config
jnius_config.add_classpath(DEBEZIUM_JAVA_LIBS_DR)
from jnius import autoclass
from jnius import PythonJavaClass, java_method, JavaMethod

################# CLASSES #################
Properties = autoclass('java.util.Properties')
DebeziumEngine = autoclass('io.debezium.engine.DebeziumEngine')
DebeziumEngineBuilder = autoclass('io.debezium.engine.DebeziumEngine$Builder')
DebeziumEngineBuilder.notifying = JavaMethod(
    '(Lio/debezium/engine/DebeziumEngine$ChangeConsumer;)Lio/debezium/engine/DebeziumEngine$Builder;')


# AsyncEmbeddedEngine = autoclass('io.debezium.embedded.async.AsyncEmbeddedEngine')
# KeyValueChangeEventFormat = autoclass('io.debezium.engine.format.KeyValueChangeEventFormat')
# ChangeConsumer = autoclass('io.debezium.engine.DebeziumEngine$ChangeConsumer')
# AsyncEngineBuilder = autoclass('io.debezium.embedded.async.AsyncEmbeddedEngine$AsyncEngineBuilder')


class RecordCommitter(ABC):
    """
    USed for type hinting, convenience
    """

    def markProcessed(self, record):
        pass

    def markBatchFinished(self):
        pass


class ChangeEvent(ABC):
    """
    USed for type hinting, convenience
    """

    def key(self) -> str:
        pass

    def value(self) -> str:
        pass

    # def headers(self) -> str:
    #     pass
    def destination(self) -> str:
        pass

    def partition(self) -> int:
        pass

class EngineFormat:
    JSON = autoclass('io.debezium.engine.format.Json')

class BaseJsonChangeConsumer(PythonJavaClass):
    __javainterfaces__ = ['io/debezium/engine/DebeziumEngine$ChangeConsumer']

    @java_method('(Ljava/util/List;Lio/debezium/engine/DebeziumEngine$RecordCommitter;)V')
    def handleBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        print(f"RECEIVED ARGUMENTS: {type(records)} {type(committer)}")
        # print(f"RECEIVED VALS: {records} {committer}")
        print(f"RECEIVED VALS: {type(records[0].value())}")
        print(f"RECEIVED VALS: {type(records[0].key())}")
        print(f"RECEIVED VALS: {(records[0].destination())}")
        print(f"RECEIVED VALS: {(records[0].value())}")
        print(f"RECEIVED VALS: {(records[0].key())}")

class DebeziumJsonEngineBuilder:
    def __init__(self):
        self.builder = DebeziumEngine.create(EngineFormat.JSON)
        self.properties: Properties = None
        self.consumer: BaseJsonChangeConsumer = None
        print(f"builder: {type(self.builder)}")

    def with_properties(self, properties: Properties):
        self.properties = properties
        return self

    def with_consumer(self, consumer: BaseJsonChangeConsumer):
        self.consumer = consumer
        return self

    def build(self) -> DebeziumEngine:
        if self.properties is None:
            raise ValueError("Please provide configuration properties!")
        if self.consumer is None:
            raise ValueError("Please provide consumer class, A python class extending BaseJsonChangeConsumer!")
        return self.builder.using(self.properties).notifying(BaseJsonChangeConsumer()).build()
