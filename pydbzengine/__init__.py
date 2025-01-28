import traceback
from abc import ABC
from pathlib import Path
from typing import List

################# INIT ####################
DEBEZIUM_JAVA_LIBS_DR = Path(__file__).parent.joinpath("debezium/libs/*").as_posix()
CLASS_PATHS = [DEBEZIUM_JAVA_LIBS_DR]
CONFIG_DIR = Path().cwd().joinpath('config')
if CONFIG_DIR.is_dir() and CONFIG_DIR.exists():
    print(f"Adding classpath: {CONFIG_DIR.as_posix()}")
    CLASS_PATHS.append(CONFIG_DIR.as_posix())

import jnius_config

# jnius_config.add_options('-Dlog4j.debug')
jnius_config.add_classpath(*CLASS_PATHS)
from jnius import autoclass
from jnius import PythonJavaClass, java_method, JavaMethod

################# CLASSES #################
Properties = autoclass('java.util.Properties')
DebeziumEngine = autoclass('io.debezium.engine.DebeziumEngine')
DebeziumEngineBuilder = autoclass('io.debezium.engine.DebeziumEngine$Builder')
DebeziumEngineBuilder.notifying = JavaMethod(
    '(Lio/debezium/engine/DebeziumEngine$ChangeConsumer;)Lio/debezium/engine/DebeziumEngine$Builder;')
StopEngineException = autoclass('io.debezium.engine.StopEngineException')
JavaLangSystem = autoclass('java.lang.System')
JavaLangThread = autoclass('java.lang.Thread')


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


class BasePythonChangeHandler(ABC):

    def handleJsonBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        raise NotImplementedError(
            "Not implemented, Please implement BasePyhonChangeConsumer and use it to consume events!")


class PythonChangeConsumer(PythonJavaClass):
    __javainterfaces__ = ['io/debezium/engine/DebeziumEngine$ChangeConsumer']

    def __init__(self):
        self.handler: BasePythonChangeHandler

    @java_method('(Ljava/util/List;Lio/debezium/engine/DebeziumEngine$RecordCommitter;)V')
    def handleBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        try:
            self.handler.handleJsonBatch(records=records, committer=committer)
        except Exception as e:
            print("ERROR: failed to consume events in python")
            print(str(e))
            print(traceback.format_exc())
            JavaLangThread.currentThread().interrupt()

    @java_method('()Z')
    def supportsTombstoneEvents(self):
        return True

    def set_change_handler(self, handler: BasePythonChangeHandler):
        self.handler = handler

    def interrupt(self):
        print("Interrupt called in python consumer")
        JavaLangThread.currentThread().interrupt()


class DebeziumJsonEngine:
    def __init__(self, properties: Properties, handler: BasePythonChangeHandler):
        self.properties: Properties = properties

        if self.properties is None:
            raise ValueError("Please provide debezium config properties!")
        if handler is None:
            raise ValueError("Please provide handler class, see example class `pydbzengine.BaseJsonChangeConsumer`!")

        self.consumer = PythonChangeConsumer()
        self._handler = handler
        self.consumer.set_change_handler(self._handler)

        self.engine: DebeziumEngine = (DebeziumEngine.create(EngineFormat.JSON)
                                       .using(self.properties)
                                       .notifying(self.consumer)
                                       .build())

    def run(self):
        self.engine.run()

    def interrupt(self):
        self.consumer.interrupt()
