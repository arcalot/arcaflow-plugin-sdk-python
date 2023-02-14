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

import cbor2

from arcaflow_plugin_sdk import schema


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


def _handle_exit(_signo, _stack_frame):
    print("Exiting normally")
    sys.exit(0)


def run_plugin(
    s: schema.SchemaType,
    stdin: io.FileIO,
    stdout: io.FileIO,
    stderr: io.FileIO,
) -> int:
    """
    This function wraps running a plugin.
    """
    if os.isatty(stdout.fileno()):
        print("Cannot run plugin in ATP mode on an interactive terminal.")
        return 1

    signal.signal(signal.SIGTERM, _handle_exit)
    try:
        decoder = cbor2.decoder.CBORDecoder(stdin)
        encoder = cbor2.encoder.CBOREncoder(stdout)

        # Decode empty "start output" message.
        decoder.decode()

        start = HelloMessage(1, s)
        serialized_message = _HELLO_MESSAGE_SCHEMA.serialize(start)
        encoder.encode(serialized_message)
        stdout.flush()

        message = decoder.decode()
    except SystemExit:
        return 0
    try:
        if message is None:
            stderr.write("Work start message is None.")
            return 1
        if message["id"] is None:
            stderr.write("Work start message is missing the 'id' field.")
            return 1
        if message["config"] is None:
            stderr.write("Work start message is missing the 'config' field.")
            return 1
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        out_buffer = io.StringIO()
        sys.stdout = out_buffer
        sys.stderr = out_buffer
        output_id, output_data = s.call_step(
            message["id"], s.unserialize_input(message["id"], message["config"])
        )
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        encoder.encode(
            {
                "output_id": output_id,
                "output_data": s.serialize_output(
                    message["id"], output_id, output_data
                ),
                "debug_logs": out_buffer.getvalue(),
            }
        )
        stdout.flush()
    except SystemExit:
        return 1
    return 0


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
