#!/usr/bin/env -S python3 -u

# Eventually command: runs only after a driver has started. When it fires,
# Antithesis KILLS all other commands and STOPS all fault injection -- this is
# the recovery check. It retries (the cluster needs time to heal after faults
# stop) until the cluster converges: all members reachable, exactly one agreed
# leader, and a probe write replicates to every member at the same revision.
# Because faults are stopped, convergence is MANDATORY -> asserted with `always`.

import sys
import time

import etcd3

from antithesis.assertions import always

sys.path.append("/opt/antithesis/resources")
import helper

HOSTS = ["etcd0", "etcd1", "etcd2"]
PORT = 2379
MAX_ATTEMPTS = 30
RETRY_INTERVAL_S = 2


def attempt_convergence():
    """One convergence sample. Returns (converged, detail)."""
    clients = {}
    for h in HOSTS:
        try:
            clients[h] = etcd3.client(host=h, port=PORT)
        except Exception as e:
            return False, f"cannot connect to {h}: {e!r}"
    if len(clients) < len(HOSTS):
        return False, "not all members reachable"

    # Exactly one leader, agreed by all members.
    leaders = set()
    for h, c in clients.items():
        try:
            leaders.add(c.status().leader.id)
        except Exception as e:
            return False, f"status failed on {h}: {e!r}"
    if len(leaders) != 1 or 0 in leaders:
        return False, f"no single agreed leader: {leaders}"

    # Probe write replicates to all members at the same value + revision.
    probe_key = "convergence/" + helper.generate_random_string()
    probe_val = helper.generate_random_string()
    ok, err = helper.put_request(clients[HOSTS[0]], probe_key, probe_val)
    if not ok:
        return False, f"probe write failed: {err}"

    revisions, values = set(), set()
    for h, c in clients.items():
        try:
            val, meta = c.get(probe_key)
        except Exception as e:
            return False, f"probe read failed on {h}: {e!r}"
        if val is None or meta is None:
            return False, f"{h} has not replicated probe yet"
        values.add(val.decode() if isinstance(val, bytes) else val)
        revisions.add(meta.mod_revision)

    if len(values) != 1 or len(revisions) != 1:
        return False, f"members disagree values={values} revisions={revisions}"

    return True, None


def wait_for_convergence():
    """Retry loop -- the cluster needs time to recover after faults stop."""
    detail = "no attempts made"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        converged, detail = attempt_convergence()
        print(f"Eventually: convergence attempt {attempt}/{MAX_ATTEMPTS} -> "
              f"{converged} ({detail})")
        if converged:
            return True, detail
        time.sleep(RETRY_INTERVAL_S)
    return False, detail


if __name__ == "__main__":
    converged, detail = wait_for_convergence()
    # Faults are stopped here -- the cluster MUST have recovered.
    always(converged, "Cluster eventually converges after faults stop",
           {"detail": detail})
    print(f"Eventually: final convergence -> {converged} ({detail})")
