import arcaflow_plugin_pb2
from google.protobuf.service import RpcChannel, RpcController


class PluginService(arcaflow_plugin_pb2.Plugin_Stub):
    def GetProtocolVersion(
            self,
            rpc_controller: RpcController,
            request: arcaflow_plugin_pb2.GetProtocolVersionRequest,
            done
    ):
        """
        GetProtocolVersion requests the current protocol version (version 1 for the current file).
        :param rpc_controller:
        :param request:
        :param done:
        :return:
        """
        pass

    def GetSchema(
            self,
            rpc_controller: RpcController,
            request: arcaflow_plugin_pb2.GetSchemaRequest,
            done
    ):
        """
        GetSchema returns the schema for this plugin. The calling side can then construct or validate data for the
        individual steps based on this schema.
        :param rpc_controller:
        :param request:
        :param done:
        :return:
        """
        pass

    def Start(self, rpc_controller: RpcController, request, done):
        """
        Start starts a workflow with the given data. Only one workflow can be started at the same time.
        :param rpc_controller:
        :param request:
        :param done:
        :return:
        """
        pass

    def Get(self, rpc_controller: RpcController, request, done):
        """
        Get returns a previously started workflow.
        """
        pass

    def Wait(self, rpc_controller: RpcController, request, done):
        """
        Wait waits for a workflow to complete instead of returning immediately.
        :param rpc_controller:
        :param request:
        :param done:
        :return:
        """
        pass



def run(
        stdin: io.TextIOWrapper,
        stdout: io.TextIOWrapper,
        stderr: io.TextIOWrapper
):
    protobuf.
