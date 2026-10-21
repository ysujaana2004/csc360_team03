"""Which node stores a key, and what AddNode / pin do to that map.

In the readings: partitioning / sharding — Think Distributed Systems ch. 7;
Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

import zlib
from platform import node_pb2
from platform.node import Node

from .base import PluginBase

class Plugin(PluginBase):
    def owner(self, node: Node, key: bytes, mode: str) -> str:
        nodes = sorted(node.nodes)
        N = len(nodes)
        if N == 0:
            return ""

        key_hash = zlib.crc32(key)

        if mode == "HASH_MOD":
            return nodes[key_hash % N]

        elif mode == "SLOTS":
            NUM_SLOTS = 1024
            slot_id = key_hash % NUM_SLOTS
            node_index = (slot_id * N) // NUM_SLOTS
            return nodes[node_index]

        elif mode == "KEY_RANGE":
            first_byte = key[0] if key else 0
            node_index = (first_byte * N) // 256
            return nodes[node_index]

        elif mode == "DIRECTORY":
            pins = getattr(node, "pins", {})
            if key in pins:
                return pins[key]
            return nodes[key_hash % N]

        elif mode == "ITEM":
            return nodes[key_hash % N]

        return nodes[0]

    def on_add_node(self, node: Node, request: node_pb2.AddNodeRequest) -> node_pb2.StatusResponse:
        if not hasattr(node, "pins"):
            node.pins = {}

        if request.peer and request.peer not in node.nodes:
            node.nodes.append(request.peer)
        if request.node_id and request.node_id not in node.nodes:
            node.nodes.append(request.node_id)

        ok = True
        if request.key:
            current_mode = getattr(request, "mode", "DIRECTORY")
            if current_mode == "ITEM":
                ok = False
            else:
                node.pins[request.key] = request.target_node or request.node_id

        modes = node_pb2.Modes(partitioning=getattr(request, "mode", "HASH_MOD"))
        self.rebalance(node, modes)

        return node_pb2.StatusResponse(ok=ok)

    def rebalance(self, node: Node, modes: node_pb2.Modes) -> None:
        current_keys = list(node.store.keys())

        for key in current_keys:
            target_owner = self.owner(node, key, modes.partitioning)
            
            if target_owner and target_owner != node.node_id:
                val = node.store[key]
                write_req = node_pb2.WriteRequest(key=key, value=val)
                try:
                    node.rpc(target_owner, "Write", write_req)
                    del node.store[key]
                except Exception:
                    pass