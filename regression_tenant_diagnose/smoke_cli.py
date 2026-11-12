#!/usr/bin/env python3
"""手工冒烟：构造双租户网络后直接调用 run_collab.py diagnose，打印真实输出。"""
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, sys.argv[1])
from collector import db as landing_db  # noqa: E402

work = tempfile.mkdtemp(prefix="rh-smoke-")
landing = os.path.join(work, "harvest.db")
net = os.path.join(work, "net.db")
mapping = os.path.join(work, "mapping.json")
doc = {"mapping_version": "smoke-v1", "institutions": [
    {"code": "INST-A", "name": "Alpha University", "aliases": []},
    {"code": "INST-B", "name": "Beta Institute", "aliases": []}]}
with open(mapping, "w", encoding="utf-8") as fh:
    json.dump(doc, fh)
conn = landing_db.connect(landing)
landing_db.init(conn)
conn.commit()
conn.close()


def add(tenant, acc, names):
    conn = landing_db.connect(landing)
    with conn:
        rid = conn.execute(
            "INSERT INTO raw_pages(tenant,scan_id,url,status,body,fetched_at)"
            " VALUES (?,?,?,200,'{}',1.0)",
            (tenant, f"s-{acc}", f"mem://{acc}")).lastrowid
        conn.execute(
            "INSERT OR IGNORE INTO result_versions(tenant,accession,revision,"
            "modified_at,retracted,payload,raw_page_id) VALUES (?,?,1,1.0,0,?,?)",
            (tenant, acc, json.dumps({"authors": [
                {"name": "n", "affiliations": names}]}), rid))
    conn.close()


add("alpha", "A1", ["Alpha University", "Mystery Alpha Lab"])
add("alpha", "A2", ["Mystery Alpha Lab", "Ghost Alpha Unit"])
add("beta", "B1", ["Beta Institute", "Mystery Beta Lab"])
run = os.path.join(sys.argv[1], "run_collab.py")
subprocess.run([sys.executable, run, "update", "--db", landing,
                "--net", net, "--mapping", mapping], check=True)

cases = [
    ["diagnose", "--net", net, "--tenant", "alpha"],
    ["diagnose", "--net", net, "--tenant", "alpha", "--json"],
    ["diagnose", "--net", net, "--tenant", "beta"],
    ["diagnose", "--net", net, "--tenant", "gamma", "--json"],
    ["diagnose", "--net", net],
    ["diagnose", "--net", net, "--tenant", "   "],
]
for c in cases:
    p = subprocess.run([sys.executable, run, *c],
                       capture_output=True, text=True)
    print("$ run_collab.py", " ".join(c), f"[exit={p.returncode}]")
    if p.stdout.strip():
        print(p.stdout.rstrip())
    if p.stderr.strip():
        print("STDERR:", p.stderr.strip().splitlines()[-1])
    print("-" * 60)
