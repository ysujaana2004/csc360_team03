"""Write/Read that arrived on a node that does not own the key.

In the readings: request routing — Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import MAX_HOP, RpcFailed, copy_with_hop, hop_count, mode

from .base import PluginBase


class Plugin(PluginBase):
    def on_write(
        self, node: Node, request: node_pb2.WriteRequest, owner: str
    ) -> node_pb2.WriteResponse | None:
        """If this node should not store the Write, forward, look up, or reject.

        Return a WriteResponse, or None to mean “handle it here.” Increment
        ``request.hop`` when forwarding; stop after 8. A stale directory lookup
        should retry once rather than drop the bytes.
        """
        if owner == node.node_id:
            return None

        m = mode(request.modes, "request_routing").upper()

        if m == "CLIENT_AWARE":
            return node_pb2.WriteResponse(ok=False, error="not owner", owner=owner)

        if m == "DIRECTORY_TIER":
            target = node.directory.get(request.key, owner)
            if target == node.node_id:
                return None

            if hop_count(request) >= MAX_HOP:
                return node_pb2.WriteResponse(ok=False, error="hop limit", owner=target)

            fwd = copy_with_hop(request)
            try:
                resp = node.rpc(target, "Write", fwd)
            except RpcFailed as exc:
                return node_pb2.WriteResponse(ok=False, error=str(exc), owner=target)
            except Exception as exc:
                return node_pb2.WriteResponse(ok=False, error=str(exc), owner=target)

            if not resp.ok and resp.error == "not owner":
                new_target = resp.owner or owner or getattr(node, "_owner", lambda k, p: "")(
                    request.key, mode(request.modes, "partitioning")
                )
                if new_target and new_target != target:
                    node.directory[request.key] = new_target
                    if new_target == node.node_id:
                        return None
                    if hop_count(fwd) >= MAX_HOP:
                        return node_pb2.WriteResponse(ok=False, error="hop limit", owner=new_target)
                    retry_fwd = copy_with_hop(fwd)
                    try:
                        resp = node.rpc(new_target, "Write", retry_fwd)
                    except RpcFailed as exc:
                        return node_pb2.WriteResponse(ok=False, error=str(exc), owner=new_target)
                    except Exception as exc:
                        return node_pb2.WriteResponse(ok=False, error=str(exc), owner=new_target)
            return resp

        # FORWARD (default mode)
        if hop_count(request) >= MAX_HOP:
            return node_pb2.WriteResponse(ok=False, error="hop limit", owner=owner)

        fwd = copy_with_hop(request)
        try:
            return node.rpc(owner, "Write", fwd)
        except RpcFailed as exc:
            return node_pb2.WriteResponse(ok=False, error=str(exc), owner=owner)
        except Exception as exc:
            return node_pb2.WriteResponse(ok=False, error=str(exc), owner=owner)

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Same idea for Read. Return a ReadResponse or None."""
        if owner == node.node_id:
            return None

        m = mode(request.modes, "request_routing").upper()

        if m == "CLIENT_AWARE":
            return node_pb2.ReadResponse(ok=False, error="not owner", served_by=node.node_id)

        if m == "DIRECTORY_TIER":
            target = node.directory.get(request.key, owner)
            if target == node.node_id:
                return None

            if hop_count(request) >= MAX_HOP:
                return node_pb2.ReadResponse(ok=False, error="hop limit", served_by=node.node_id)

            fwd = copy_with_hop(request)
            try:
                resp = node.rpc(target, "Read", fwd)
            except RpcFailed as exc:
                return node_pb2.ReadResponse(ok=False, error=str(exc), served_by=node.node_id)
            except Exception as exc:
                return node_pb2.ReadResponse(ok=False, error=str(exc), served_by=node.node_id)

            if not resp.ok and resp.error == "not owner":
                new_target = owner or getattr(node, "_owner", lambda k, p: "")(
                    request.key, mode(request.modes, "partitioning")
                )
                if new_target and new_target != target:
                    node.directory[request.key] = new_target
                    if new_target == node.node_id:
                        return None
                    if hop_count(fwd) >= MAX_HOP:
                        return node_pb2.ReadResponse(ok=False, error="hop limit", served_by=node.node_id)
                    retry_fwd = copy_with_hop(fwd)
                    try:
                        resp = node.rpc(new_target, "Read", retry_fwd)
                    except RpcFailed as exc:
                        return node_pb2.ReadResponse(ok=False, error=str(exc), served_by=node.node_id)
                    except Exception as exc:
                        return node_pb2.ReadResponse(ok=False, error=str(exc), served_by=node.node_id)
            return resp

        # FORWARD (default mode)
        if hop_count(request) >= MAX_HOP:
            return node_pb2.ReadResponse(ok=False, error="hop limit", served_by=node.node_id)

        fwd = copy_with_hop(request)
        try:
            return node.rpc(owner, "Read", fwd)
        except RpcFailed as exc:
            return node_pb2.ReadResponse(ok=False, error=str(exc), served_by=node.node_id)
        except Exception as exc:
            return node_pb2.ReadResponse(ok=False, error=str(exc), served_by=node.node_id)
