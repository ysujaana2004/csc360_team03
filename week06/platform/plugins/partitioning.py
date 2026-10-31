"""Which node stores a key, and what AddNode / pin do to that map."""
from __future__ import annotations

import zlib
from platform import node_pb2
from platform.node import Node

from .base import PluginBase


def _order(nodes):
    # same ordering node.py uses, so n10 sorts after n2
    return sorted(nodes, key=lambda x: (len(x), x))


class Plugin(PluginBase):
    def owner(self, node: Node, key: bytes, mode: str) -> str:
        nodes = _order(node.nodes)
        n = len(nodes)
        if n == 0:
            return ""

        h = zlib.crc32(key)

        if mode == "HASH_MOD":
            return nodes[h % n]

        if mode == "SLOTS":
            slots = getattr(node, "n_slots", 32) or 32
            slot = h % slots
            return nodes[(slot * n) // slots]

        if mode == "KEY_RANGE":
            first = key[0] if key else 0
            return nodes[(first * n) // 256]

        if mode == "DIRECTORY":
            pinned = node.directory.get(key)
            if pinned:
                return pinned
            return nodes[h % n]

        if mode == "ITEM":
            return nodes[h % n]

        return nodes[0]  # no mode set yet: boot default (first node)

    def on_add_node(self, node: Node, request: node_pb2.AddNodeRequest) -> node_pb2.StatusResponse:
        part = request.modes.partitioning or node.last_partitioning
        ok = True

        with node.lock:
            if request.node_id and request.node_id not in node.nodes:
                node.nodes.append(request.node_id)
                node.nodes.sort(key=lambda x: (len(x), x))
            if request.node_id and request.host:
                node.peers[request.node_id] = (request.host, int(request.port or 0))
            if request.modes.partitioning:
                node.last_partitioning = request.modes.partitioning

            if request.pin_key:
                if part == "DIRECTORY" and request.node_id:
                    node.directory[request.pin_key] = request.node_id
                else:
                    ok = False  # pins only work in DIRECTORY mode

        self.rebalance(node, node_pb2.Modes(partitioning=part))

        st = node.status_now()
        st.ok = ok
        return st

    def rebalance(self, node: Node, modes: node_pb2.Modes) -> None:
        part = modes.partitioning or node.last_partitioning
        if not part:
            return
        if modes.partitioning:
            node.last_partitioning = modes.partitioning

        with node.lock:
            items = list(node.store.items())

        for key, val in items:
            target = self.owner(node, key, part)
            if not target or target == node.node_id:
                continue
            req = node_pb2.WriteRequest(
                key=key, value=val, modes=node_pb2.Modes(partitioning=part)
            )
            try:
                resp = node.rpc(target, "Write", req)
            except Exception:
                continue
            if getattr(resp, "ok", False):
                with node.lock:
                    node.store.pop(key, None)