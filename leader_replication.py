from platform import node_pb2
from platform.util import RpcFailed, copy_with_hop, mode

from .base import PluginBase


class Plugin(PluginBase):
    def admit(self, node, request, owner):
        """Decide if this node can take the Write, or send it to the leader."""
        m = mode(request.modes, "leader_replication")
        if m == "LEADERLESS" or m == "MULTI_LEADER":
            return None
        if m != "SINGLE_LEADER":
            raise NotImplementedError
        if node.node_id == node.leader:
            return None
        return self.forward(node, request)

    def after_local_write(self, node, request, value, owner):
        """After a Write is saved, copy it to the other nodes."""
        m = mode(request.modes, "leader_replication")
        if m == "MULTI_LEADER" or (m == "SINGLE_LEADER" and node.node_id == node.leader):
            self.copy(node, request, value)

    def forward(self, node, request):
        try:
            return node.rpc(node.leader, "Write", copy_with_hop(request))
        except RpcFailed:
            return node_pb2.WriteResponse(ok=False, error="not leader", owner=node.leader)

    def copy(self, node, request, value):
        req = node_pb2.ReplicateRequest(
            key=request.key,
            value=value,
            kind="write",
            index=len(node.log) - 1,
            source=node.node_id,
            modes=request.modes,
        )
        for peer in node.other_nodes():
            try:
                node.rpc(peer, "Replicate", req)
            except RpcFailed:
                pass
