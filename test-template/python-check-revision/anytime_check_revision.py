#!/usr/bin/env -S python3 -u

# Anytime command: runs at any moment, concurrently with drivers, WITH faults
# active. Checks a safety invariant that must hold continuously -- etcd's
# revision never goes backwards. On a single member, writes a probe key several
# times and confirms each write's mod_revision strictly increases.

import sys

import etcd3

from antithesis.assertions import always, sometimes

sys.path.append("/opt/antithesis/resources")
import helper

HOSTS = ["etcd0", "etcd1", "etcd2"]
PORT = 2379
WRITES = 5


def check_revision_monotonic():
    client = helper.connect_to_host()
    key = "revision/" + helper.generate_random_string()

    last_rev = -1
    for _ in range(WRITES):
        value = helper.generate_random_string()
        ok, err = helper.put_request(client, key, value)
        # A failed write is acceptable under faults; skip it.
        sometimes(ok, "Client can make successful put requests", {"error": err})
        if not ok:
            continue
        try:
            _val, meta = client.get(key)
        except Exception as e:
            return False, f"read-back failed: {e!r}"
        if meta is None:
            return False, "no metadata on read-back"
        rev = meta.mod_revision
        if rev <= last_rev:
            return False, f"revision went backwards: {rev} after {last_rev}"
        last_rev = rev

    return True, None


if __name__ == "__main__":
    ok, detail = check_revision_monotonic()
    # Safety invariant -- must hold even mid-fault.
    always(ok, "Revision is monotonically increasing", {"detail": detail})
    print(f"Anytime: revision-monotonic check -> {ok} ({detail})")