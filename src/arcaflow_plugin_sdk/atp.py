"""
The Arcaflow Transport Protocol is a protocol to communicate between the Engine and a plugin. This library provides
the services to do so. The main use-case is running over STDIO, so the communication protocol is designed to run 1:1,
it is not possible to use multiple clients (engines) with a plugin.

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
import typing
import threading

import cbor2

from enum import Enum

from arcaflow_plugin_sdk import schema

class MessageType(Enum):
    WORKDONE = 1
    SIGNAL = 2


@dataclasses.dataclass
class HelloMessage:
    """
    This message is the the initial greeting message a plugin sends to the output.
    """

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
            "Schema information describing the required inputs and outputs, as well as the steps offered by this "
            "plugin."
        ),
    ]


_HELLO_MESSAGE_SCHEMA = schema.build_object_schema(HelloMessage)

class ATPServer:

    stdin: io.FileIO
    stdout: io.FileIO
    stderr: io.FileIO
    step_object: typing.Any

    def __init__(self,
        stdin: io.FileIO,
        stdout: io.FileIO,
        stderr: io.FileIO,
    ) -> None:
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

    def run_plugin(
        self,
        s: schema.SchemaType,
    ) -> int:
        """
        This function wraps running a plugin.
        """
        if os.isatty(self.stdout.fileno()):
            print("Cannot run plugin in ATP mode on an interactive terminal.")
            return 1
        try:
            decoder = cbor2.decoder.CBORDecoder(self.stdin)
            encoder = cbor2.encoder.CBOREncoder(self.stdout)

            # Decode empty "start output" message.
            decoder.decode()

            # Serialize then send HelloMessage
            start_hello_message = HelloMessage(2, s)
            serialized_message = _HELLO_MESSAGE_SCHEMA.serialize(start_hello_message)
            encoder.encode(serialized_message)
            self.stdout.flush()

            # Can fail here if only getting schema.
            work_start_msg = decoder.decode()
        except SystemExit:
            return 0
        try:
            if work_start_msg is None:
                self.stderr.write("Work start message is None.")
                return 1
            if work_start_msg["id"] is None:
                self.stderr.write("Work start message is missing the 'id' field.")
                return 1
            if work_start_msg["config"] is None:
                self.stderr.write("Work start message is missing the 'config' field.")
                return 1
            # Run the read loop
            read_thread = threading.Thread(target=self.run_server_read_loop, args=(self, decoder, self.stderr))
            read_thread.start()
            # Run the step 
            original_stdout = sys.stdout
            original_stderr = sys.stderr
            out_buffer = io.StringIO()
            sys.stdout = out_buffer
            sys.stderr = out_buffer
            output_id, output_data = s.call_step(
                work_start_msg["id"], s.unserialize_input(work_start_msg["id"], work_start_msg["config"])
            )
            sys.stdout = original_stdout
            sys.stderr = original_stderr

            # Send WorkDoneMessage in a RuntimeMessage
            encoder.encode(
                {
                    "id": MessageType.WORKDONE,
                    "data": {
                        "output_id": output_id,
                        "output_data": s.serialize_output(
                            work_start_msg["id"], output_id, output_data
                        ),
                        "debug_logs": out_buffer.getvalue(),
                    }
                }
            )
            self.stdout.flush()
            read_thread.join()
        except SystemExit:
            return 1
        return 0

    def run_server_read_loop(
        self,
        step: schema.SchemaType,
        step_id: str,
        decoder: cbor2.decoder.CBORDecoder,
        stderr: io.FileIO
    ) -> None:
        try:
            while True:
                # Decode the message
                runtime_msg = decoder.decode()
                msg_id = runtime_msg["id"]
                # Validate
                if msg_id is None:
                    stderr.write("Runtime message is missing the 'id' field.")
                # Then take action
                if msg_id == MessageType.SIGNAL:
                    signal_msg = runtime_msg["data"]
                    received_step_id = signal_msg["step_id"]
                    received_signal_id = signal_msg["signal_id"]
                    if received_step_id != step_id:
                        stderr.write(f"Received step ID in the signal message '{received_step_id}'"
                                     f"does not match expected step ID '{step_id}'")
                        return
                    step.call_step_signal(step_id, received_signal_id, signal_msg["data"])
                else:
                    stderr.write(f"Unknown kind of runtime message: {msg_id}")

        except cbor2.CBORDecodeError:
            stderr.write(f"Error while decoding CBOR: {msg_id}")


class PluginClientStateException(Exception):
    """
    This
    """

    msg: str

    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return self.msg


class PluginClient:
    """
    This is a rudimentary client that reads information from a plugin and starts work on the plugin. The functions
    must be executed in order.
    """

    stdin: io.FileIO
    stdout: io.FileIO
    decoder: cbor2.decoder.CBORDecoder

    def __init__(
        self,
        stdin: io.FileIO,
        stdout: io.FileIO,
    ):
        self.stdin = stdin
        self.stdout = stdout
        self.decoder = cbor2.decoder.CBORDecoder(stdout)
        self.encoder = cbor2.encoder.CBOREncoder(stdin)

    def start_output(self) -> None:
        self.encoder.encode(None)

    def read_hello(self) -> HelloMessage:
        """
        This function reads the intial "Hello" message from the plugin.
        """
        message = self.decoder.decode()
        return _HELLO_MESSAGE_SCHEMA.unserialize(message)

    def start_work(self, step_id: str, input_data: any):
        """
        After the Hello message has been read, this function starts work in a plugin with the specified data.
        """
        self.encoder.encode(
            {
                "id": step_id,
                "config": input_data,
            }
        )

    def read_results(self) -> (str, any, str):
        """
        This function reads the results of an execution from the plugin.
        """
        message = self.decoder.decode()
        if message["output_id"] is None:
            raise PluginClientStateException(
                "Missing 'output_id' in CBOR message. Possibly wrong order of calls?"
            )
        if message["output_data"] is None:
            raise PluginClientStateException(
                "Missing 'output_data' in CBOR message. Possibly wrong order of calls?"
            )
        if message["debug_logs"] is None:
            raise PluginClientStateException(
                "Missing 'output_data' in CBOR message. Possibly wrong order of calls?"
            )
        return message["output_id"], message["output_data"], message["debug_logs"]
