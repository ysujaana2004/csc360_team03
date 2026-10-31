"""When Write returns: send copies, wait for W, time out if a node is paused.

In the readings: synchronous / asynchronous / hybrid (quorum) replication —
Think Distributed Systems §8.4.3; Database Internals ch. 11.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import RpcFailed, mode

from .base import PluginBase


class Plugin(PluginBase):
    def _pending(self, node: Node):
        pending = getattr(node, "_write_completion_pending", None)
        if pending is None:
            pending = {}
            setattr(node, "_write_completion_pending", pending)
        return pending

    def _queue_pending(self, node: Node, peer: str, key: bytes, value: bytes, modes: node_pb2.Modes) -> None:
        pending = self._pending(node)
        peer_pending = pending.setdefault(peer, [])
        req = node_pb2.ReplicateRequest(
            key=key,
            value=value,
            kind="write",
            index=len(node.log),
            source=node.node_id,
            modes=modes,
        )
        for item in peer_pending:
            if item[0] == key and item[1] == value:
                return
        peer_pending.append((key, value, req))

    def _install_locally(self, node: Node, key: bytes, value: bytes) -> None:
        with node.lock:
            node.store[key] = value
            index = len(node.log)
            node.log.append((index, key, value))
            node.last_applied = index

    def spread(
        self,
        node: Node,
        key: bytes,
        value: bytes,
        owner: str,
        modes: node_pb2.Modes,
    ) -> node_pb2.WriteResponse:
        """Install locally (via the replication plugin), send copies, wait for W acks.

        W comes from the current membership ``len(node.nodes)``: 1, majority
        (``N//2 + 1``), or all N. Return a WriteResponse: ok if W members acked,
        otherwise ``ack timeout``. Remember failed copies so ``repair`` / ``on_read``
        can finish an incomplete write after the peer is reachable again.
        """
        mode_name = mode(modes, "write_completion")
        n = len(node.nodes)
        if mode_name == "ACK_ONE":
            required = 1
        elif mode_name == "ACK_QUORUM":
            required = (n // 2) + 1
        elif mode_name == "ACK_ALL":
            required = n
        else:
            required = 1

        self._install_locally(node, key, value)
        acked_by = [node.node_id]
        if required <= 1:
            return node_pb2.WriteResponse(ok=True, owner=owner, acked_by=acked_by)

        for peer in node.other_nodes():
            if len(acked_by) >= required:
                break
            req = node_pb2.ReplicateRequest(
                key=key,
                value=value,
                kind="write",
                index=len(node.log) - 1,
                source=node.node_id,
                modes=modes,
            )
            try:
                resp = node.rpc(peer, "Replicate", req, timeout_s=2.0)
            except (RpcFailed, Exception):
                self._queue_pending(node, peer, key, value, modes)
                continue
            if getattr(resp, "ok", False):
                acked_by.append(peer)
            else:
                self._queue_pending(node, peer, key, value, modes)

        if len(acked_by) >= required:
            return node_pb2.WriteResponse(ok=True, owner=owner, acked_by=acked_by)
        return node_pb2.WriteResponse(ok=False, error="ack timeout", owner=owner, acked_by=acked_by)

    def repair(self, node: Node, modes: node_pb2.Modes) -> None:
        """Hinted handoff: retry copies that timed out while a peer was paused."""
        pending = self._pending(node)
        for peer in list(pending):
            for item in list(pending[peer]):
                key, value, req = item
                try:
                    resp = node.rpc(peer, "Replicate", req, timeout_s=2.0)
                except (RpcFailed, Exception):
                    continue
                if getattr(resp, "ok", False):
                    pending[peer].remove(item)
            if not pending[peer]:
                del pending[peer]

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Read repair: if this node is missing ``key``, pull from a peer and store it."""
        with node.lock:
            local = node.store.get(request.key)
            if local is not None:
                return None

        for peer in node.other_nodes():
            try:
                resp = node.rpc(
                    peer,
                    "Read",
                    node_pb2.ReadRequest(
                        key=request.key,
                        session_id=request.session_id,
                        modes=request.modes,
                        hop=request.hop,
                    ),
                    timeout_s=2.0,
                )
            except (RpcFailed, Exception):
                continue
            if getattr(resp, "ok", False) and getattr(resp, "found", False):
                with node.lock:
                    node.store[request.key] = resp.value
                    if not any(rec[1] == request.key and rec[2] == resp.value for rec in node.log):
                        node.log.append((len(node.log), request.key, resp.value))
                    node.last_applied = max(node.last_applied, len(node.log) - 1)
                return node_pb2.ReadResponse(
                    ok=True,
                    value=resp.value,
                    found=True,
                    served_by=node.node_id,
                )

        return None
