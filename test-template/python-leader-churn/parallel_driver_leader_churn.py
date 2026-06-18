#!/usr/bin/env -S python3 -u

# This file serves as a parallel driver. It performs a single Raft leadership
# transfer: it finds the current leader and moves leadership to another member
# chosen via Antithesis randomness, so the platform owns and can replay that
# branch point. It generates no traffic of its own -- the platform overlaps it
# with the traffic driver and with fault injection however it chooses.

import sys

import etcd3

# Antithesis SDK
from antithesis.assertions import sometimes
from antithesis.random import get_random

sys.path.append("/opt/antithesis/resources")
import helper

HOSTS = ["etcd0", "etcd1", "etcd2"]
PORT = 2379


def get_leader_info():
    """Returns (leader_id, [members]) by asking any reachable host."""
    for h in HOSTS:
        try:
            c = etcd3.client(host=h, port=PORT)
            st = c.status()
            return st.leader.id, list(c.members)
        except Exception as e:
            print(f"Driver: status check failed on {h}: {e}")
    return None, None


def move_leader():
    """Transfer leadership from the current leader to another member."""
    leader_id, members = get_leader_info()
    if leader_id is None or not members:
        print("Driver: could not determine leader; skipping transfer")
        return False, "no leader"

    by_id = {m.id: m for m in members}
    leader = by_id.get(leader_id)
    candidates = [m for m in members if m.id != leader_id]
    if not leader or not candidates:
        return False, "no transfer target"

    # Antithesis randomness: let the platform own which member we target.
    target = candidates[get_random() % len(candidates)]
    leader_host = leader.name or HOSTS[0]
    print(f"Driver: current leader is {leader.name} ({leader_id:x})")

    try:
        import etcd3.etcdrpc as etcdrpc
        lc = etcd3.client(host=leader_host, port=PORT)
        req = etcdrpc.MoveLeaderRequest(targetID=target.id)
        lc.maintenancestub.MoveLeader(req, lc.timeout, metadata=lc.metadata)
        print(f"Driver: requested leadership move {leader.name} -> {target.name}")
        return True, None
    except Exception as e:
        print(f"Driver: leadership transfer failed: {e!r}")
        return False, repr(e)


if __name__ == "__main__":
    success, error = move_leader()
    # Antithesis Assertion: sometimes leadership transfers succeed. A failure is
    # OK -- the target may be partitioned or unreachable, which is the point.
    sometimes(success, "Leadership can be transferred between members", {"error": error})
    print("Driver: churn step done!")