import grpc

import yarntf.clusterspecgenerator_pb2 as csg
import yarntf.clusterspecgenerator_pb2_grpc as csg_grpc


class ClusterSpecGeneratorClient:
  def __init__(self, target):
    self.channel = grpc.insecure_channel(target)
    self.stub = csg_grpc.ClusterSpecGeneratorStub(self.channel)

  def register_container(self, application_id, ip, port, job_name, task_index, tb_port):
    container = csg.Container()
    container.applicationId = application_id
    container.ip = ip
    container.port = port
    container.jobName = job_name
    container.taskIndex = task_index
    container.tbPort = tb_port
    request = csg.RegisterContainerRequest(container=container)
    try:
      self.stub.RegisterContainer(request)
    except grpc.RpcError:
      return False
    return True

  def get_cluster_spec(self, application_id):
    request = csg.GetClusterSpecRequest()
    request.applicationId = application_id
    try:
      reply = self.stub.GetClusterSpec(request)
    except grpc.RpcError:
      return None
    return reply.clusterSpec
