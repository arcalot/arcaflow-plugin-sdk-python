"""The Arcaflow Transport Protocol is a protocol to communicate between the
Engine and a plugin. This library provides the services to do so. The main use-
case is running over STDIO, so the communication protocol is designed to run
1:1, it is not possible to use multiple clients (engines) with a plugin.

The message flow is as follows:

|   sequenceDiagram
|     participant E as Engine
|       participant P as Plugin
|       autonumber
|       P ->> E: Schema information
|       E ->> P: Start step
|       P ->> E: Execution results
"""
import dataclasses
import io
import os
import signal
import sys
import threading
import traceback
import typing
from enum import IntEnum

import cbor2

from arcaflow_plugin_sdk import schema

ATP_SERVER_VERSION = 3


class MessageType(IntEnum):
    """An integer ID that indicates the type of runtime message that is stored
    in the data field.

    The corresponding class can then be used to deserialize the inner data.
    Look at the go SDK for the reference data structure.
    """

    WORK_START = 1
    WORK_DONE = 2
    SIGNAL = 3
    CLIENT_DONE = 4
    ERROR = 5


@dataclasses.dataclass
class HelloMessage:
    """This message is the initial greeting message a plugin sends to the
    output."""

    version: typing.Annotated[
        int,
        schema.min(1),
        schema.name("Version"),
        schema.description("Version number for the protocol"),
    ]
    schema: typing.Annotated[
        schema.Schema,
        schema.name("Schema"),
        schema.description(
            "Schema information describing the required inputs and outputs, as"
            " well as the steps offered by this plugin."
        ),
    ]


_HELLO_MESSAGE_SCHEMA = schema.build_object_schema(HelloMessage)


class ATPServer:
    input_pipe: io.FileIO
    output_pipe: io.FileIO
    stderr: io.FileIO
    step_ids: typing.Dict[str, str]  # Run ID to step IDs
    encoder: cbor2.CBOREncoder
    decoder: cbor2.CBORDecoder
    user_out_buffer: io.StringIO
    encoder_lock: threading.Lock
    plugin_schema: schema.SchemaType
    running_threads: typing.List[threading.Thread]

    def __init__(
        self,
        input_pipe: io.FileIO,
        output_pipe: io.FileIO,
        stderr: io.FileIO,
    ) -> None:
        self.input_pipe = input_pipe
        self.output_pipe = output_pipe
        self.stderr = stderr
        self.step_ids = {}
        self.encoder_lock = threading.Lock()
        self.running_threads = []

    def run_plugin(
        self,
        plugin_schema: schema.SchemaType,
    ) -> int:
        """This function wraps running a plugin."""
        signal.signal(
            signal.SIGINT, signal.SIG_IGN
        )  # Ignore sigint. Only care about arcaflow signals.
        if os.isatty(self.output_pipe.fileno()):
            print("Cannot run plugin in ATP mode on an interactive terminal.")
            return 1
        self.decoder = cbor2.CBORDecoder(self.input_pipe)
        self.encoder = cbor2.CBOREncoder(self.output_pipe)
        self.plugin_schema = plugin_schema
        self.handle_handshake()

        # First replace stdout so that prints are handled by us, instead of
        # potentially interfering with the atp pipes.
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        self.user_out_buffer = io.StringIO()
        sys.stdout = self.user_out_buffer
        sys.stderr = self.user_out_buffer

        # Run the read loop. This blocks to wait for the loop to finish.
        self.run_server_read_loop()
        # Wait for the step/signal threads to finish. If it gets stuck here
        # then there is another thread blocked.
        for thread in self.running_threads:
            thread.join()

        # Don't reset stdout/stderr until after the read and step/signal
        # threads are done.
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        return 0

    def handle_handshake(self):
        # Decode empty "start output" message.
        self.decoder.decode()

        # Serialize then send HelloMessage
        start_hello_message = HelloMessage(
            ATP_SERVER_VERSION, self.plugin_schema
        )
        serialized_message = _HELLO_MESSAGE_SCHEMA.serialize(
            start_hello_message
        )
        self.send_message(serialized_message)

    def run_server_read_loop(self) -> None:
        try:
            while True:
                # Decode the message
                runtime_msg = self.decoder.decode()
                msg_id = runtime_msg.get("id", None)
                # Validate
                if msg_id is None:
                    self.send_error_message(
                        "",
                        step_fatal=False,
                        server_fatal=True,
                        error_msg="Runtime message is missing the 'id' field.",
                    )
                    return
                run_id = runtime_msg["run_id"]
                # Then take action
                if msg_id == MessageType.WORK_START:
                    work_start_msg = runtime_msg["data"]
                    try:
                        self.handle_work_start(run_id, work_start_msg)
                    except Exception as e:
                        self.send_error_message(
                            run_id,
                            step_fatal=True,
                            server_fatal=False,
                            error_msg=(
                                "Exception while handling work start: "
                                f"{e} {traceback.format_exc()}"
                            ),
                        )
                elif msg_id == MessageType.SIGNAL:
                    signal_msg = runtime_msg["data"]
                    try:
                        self.handle_signal(run_id, signal_msg)
                    except Exception as e:
                        self.send_error_message(
                            run_id,
                            step_fatal=False,
                            server_fatal=False,
                            error_msg=(
                                "Exception while handling signal:"
                                f" {e} {traceback.format_exc()}"
                            ),
                        )
                elif msg_id == MessageType.CLIENT_DONE:
                    return
                else:
                    self.send_error_message(
                        run_id,
                        step_fatal=False,
                        server_fatal=False,
                        error_msg=f"Unknown runtime message ID: {msg_id}",
                    )
                    self.stderr.write(bytes(
                        f"Unknown kind of runtime message: {msg_id}",
                        encoding="utf")
                    )

        except cbor2.CBORDecodeError as err:
            self.stderr.write(bytes(
                f"Error while decoding CBOR: {err}", encoding="utf"))
            self.send_error_message(
                "",
                step_fatal=False,
                server_fatal=True,
                error_msg=(
                    "Error occurred while decoding CBOR:"
                    f" {err} {traceback.format_exc()}"
                ),
            )
        except Exception as e:
            self.send_error_message(
                "",
                step_fatal=False,
                server_fatal=True,
                error_msg=(
                    "Exception occurred in ATP server read loop:"
                    f" {e} {traceback.format_exc()}"
                ),
            )

    def handle_signal(self, run_id, signal_msg):
        saved_step_id = self.step_ids[run_id]
        received_signal_id = signal_msg["signal_id"]

        unserialized_data = (
            self.plugin_schema.unserialize_signal_handler_input(
                saved_step_id, received_signal_id, signal_msg["data"]
            )
        )
        # The data is verified and unserialized. Now call the signal in its own
        # thread.
        run_thread = threading.Thread(
            target=self.run_signal,
            args=(
                run_id,
                saved_step_id,
                received_signal_id,
                unserialized_data,
            ),
        )
        self.running_threads.append(run_thread)
        run_thread.start()

    def run_signal(
        self,
        run_id: str,
        step_id: str,
        signal_id: str,
        unserialized_input_param: any,
    ):
        try:
            self.plugin_schema.call_step_signal(
                run_id, step_id, signal_id, unserialized_input_param
            )
        except Exception as e:
            self.send_error_message(
                run_id,
                step_fatal=False,
                server_fatal=False,
                error_msg=(
                    "Error while calling signal for step with run ID"
                    f" {run_id}: {e} {traceback.format_exc()}"
                ),
            )

    def handle_work_start(
        self, run_id: str, work_start_msg: typing.Dict[str, any]
    ):
        if work_start_msg is None:
            self.send_error_message(
                run_id,
                step_fatal=True,
                server_fatal=False,
                error_msg="Work start message is None.",
            )
            return
        if "id" not in work_start_msg:
            self.send_error_message(
                run_id,
                step_fatal=True,
                server_fatal=False,
                error_msg="Work start message is missing the 'id' field.",
            )
            return
        if "config" not in work_start_msg:
            self.send_error_message(
                run_id,
                step_fatal=True,
                server_fatal=False,
                error_msg="Work start message is missing the 'config' field.",
            )
            return
        # Save for later
        self.step_ids[run_id] = work_start_msg["id"]

        # Now run the step, so start in a new thread
        run_thread = threading.Thread(
            target=self.start_step,
            args=(
                run_id,
                work_start_msg["id"],
                work_start_msg["config"],
            ),
        )
        self.running_threads.append(
            run_thread
        )  # Save so that we can join with it at the end.
        run_thread.start()

    def start_step(self, run_id: str, step_id: str, config: typing.Any):
        try:
            output_id, output_data = self.plugin_schema.call_step(
                run_id,
                step_id,
                self.plugin_schema.unserialize_step_input(step_id, config),
            )

            # Send WorkDoneMessage
            self.send_runtime_message(
                MessageType.WORK_DONE,
                run_id,
                {
                    "output_id": output_id,
                    "output_data": self.plugin_schema.serialize_output(
                        step_id, output_id, output_data
                    ),
                    "debug_logs": self.user_out_buffer.getvalue(),
                },
            )
        except Exception as e:
            self.send_error_message(
                run_id,
                step_fatal=True,
                server_fatal=False,
                error_msg=(
                    f"Error while calling step {run_id}/{step_id}:"
                    f"{e} {traceback.format_exc()}"
                ),
            )
            return

    def send_message(self, data: any):
        with self.encoder_lock:
            self.encoder.encode(data)
            self.output_pipe.flush()  # Sends it to the ATP client immediately.

    def send_runtime_message(
        self, message_type: MessageType, run_id: str, data: any
    ):
        self.send_message(
            {
                "id": message_type,
                "run_id": run_id,
                "data": data,
            }
        )

    def send_error_message(
        self, run_id: str, step_fatal: bool, server_fatal: bool, error_msg: str
    ):
        self.send_runtime_message(
            MessageType.ERROR,
            run_id,
            {
                "error": error_msg,
                "step_fatal": step_fatal,
                "server_fatal": server_fatal,
            },
        )


class PluginClientStateException(Exception):
    """This exception is for client ATP client errors, like problems
    decoding."""

    msg: str

    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return self.msg


@dataclasses.dataclass
class StepResult:
    run_id: str
    output_id: str
    output_data: any
    debug_logs: str


class PluginClient:
    """This is a rudimentary client that reads information from a plugin and
    starts work on the plugin.

    The functions must be executed in order.
    """

    to_server_pipe: io.FileIO  # Usually the stdin of the sub-process
    to_client_pipe: io.FileIO  # Usually the stdout of the sub-process
    decoder: cbor2.CBORDecoder

    def __init__(
        self,
        to_server_pipe: io.FileIO,
        to_client_pipe: io.FileIO,
    ):
        self.to_server_pipe = to_server_pipe
        self.to_client_pipe = to_client_pipe
        self.decoder = cbor2.CBORDecoder(to_client_pipe)
        self.encoder = cbor2.CBOREncoder(to_server_pipe)

    def start_output(self) -> None:
        self.encoder.encode(None)

    def read_hello(self) -> HelloMessage:
        """This function reads the initial "Hello" message from the plugin."""
        message = self.decoder.decode()
        return _HELLO_MESSAGE_SCHEMA.unserialize(message)

    def start_work(self, run_id: str, step_id: str, config: any):
        """After the Hello message has been read, this function starts work in
        a plugin with the specified data."""
        self.send_runtime_message(
            MessageType.WORK_START,
            run_id,
            {
                "id": step_id,
                "config": config,
            },
        )

    def send_signal(self, run_id: str, signal_id: str, input_data: any):
        """This function sends any signals to the plugin."""
        self.send_runtime_message(
            MessageType.SIGNAL,
            run_id,
            {
                "signal_id": signal_id,
                "data": input_data,
            },
        )

    def send_client_done(self):
        self.send_runtime_message(MessageType.CLIENT_DONE, "", {})

    def send_runtime_message(
        self, message_type: MessageType, run_id: str, data: any
    ):
        self.encoder.encode(
            {
                "id": message_type,
                "run_id": run_id,
                "data": data,
            }
        )
        self.to_server_pipe.flush()

    def read_single_result(self) -> StepResult:
        """This function reads until it gets the next result of an execution
        from the plugin, or an error."""
        while True:
            runtime_msg = self.decoder.decode()
            msg_id = runtime_msg["id"]
            if msg_id == MessageType.WORK_DONE:
                signal_msg = runtime_msg["data"]
                if signal_msg["output_id"] is None:
                    raise PluginClientStateException(
                        "Missing 'output_id' in CBOR message. Possibly wrong"
                        " order of calls?"
                    )
                if signal_msg["output_data"] is None:
                    raise PluginClientStateException(
                        "Missing 'output_data' in CBOR message. Possibly wrong"
                        " order of calls?"
                    )
                if signal_msg["debug_logs"] is None:
                    raise PluginClientStateException(
                        "Missing 'output_data' in CBOR message. Possibly wrong"
                        " order of calls?"
                    )
                return StepResult(
                    run_id=runtime_msg["run_id"],
                    output_id=signal_msg["output_id"],
                    output_data=signal_msg["output_data"],
                    debug_logs=signal_msg["debug_logs"],
                )
            elif msg_id == MessageType.SIGNAL:
                # Do nothing. Should change in the future.
                continue
            elif msg_id == MessageType.ERROR:
                raise PluginClientStateException(
                    "Error received from ATP Server (plugin): "
                    + str(runtime_msg["data"]).replace("\\n", "\n")
                )
            else:
                raise PluginClientStateException(
                    f"Received unknown runtime message ID {msg_id}"
                )
