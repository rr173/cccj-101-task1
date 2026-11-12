#!/usr/bin/env python3
"""租户级 diagnose 缺口排查的回归断言（可从干净副本运行，仅用标准库）。

背景：合作网 CLI 的 diagnose 命令历史上没有租户入口，一次运行会把多个数据
所有者（租户）的未映射单位名称与成果编号混合输出，且传入 --tenant 会被
argparse 直接拒绝。本脚本固定修复后的契约：

  * diagnose 必须显式携带非空白的 --tenant，缺参/空白值一律退出码 2，
    任何情况下都不得回退成跨租户全局查询；
  * 文本与 JSON 输出只包含指定租户的未映射名称、涉及成果数与成果编号；
  * 指定租户没有待处理名称（或该租户根本不存在）时返回退出码 0 的正常
    空结果，而不是其他租户的数据；
  * 其他查询命令（neighbors/status 等）、网络计算与原始落地库保持原样。

用法：
    python3 -I run_tenant_diagnose_regression.py --project /path/to/research_harvester

脚本不向项目目录写入任何文件：落地库、对照表、网络库全部建在临时目录中，
退出时清理。退出码 0 表示全部断言通过；任何断言失败退出码 1。

脚本同时会打印“修复前行为取证”一节（不参与断言），如实记录当前二进制对
“不带 --tenant”和“带 --tenant”两种调用的实际响应，便于在修复前后对照。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time

FAILURES = []


def check(cond, label, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        FAILURES.append(label)


def section(name):
    print(f"\n=== {name} ===")


# 两个租户各自拥有不同的未映射名称与互不重叠的成果编号 -----------------------
MAPPING_DOC = {
    "mapping_version": "tenant-diagnose-regression-v1",
    "institutions": [
        {"code": "INST-A", "name": "Alpha University",
         "aliases": ["Alpha Univ"]},
        {"code": "INST-B", "name": "Beta Institute", "aliases": []},
    ],
}

ALPHA_NAMES = {"Alpha Unknown One", "Alpha Unknown Two"}
BETA_NAMES = {"Beta Unknown One", "Beta Unknown Two"}
ALPHA_ACCS = {"A1", "A2", "A3"}
BETA_ACCS = {"B1", "B2"}

# 期望的诊断结果：名称 -> (成果数, 有序成果编号)
EXPECT_ALPHA = {
    "Alpha Unknown One": (2, ["A1", "A2"]),
    "Alpha Unknown Two": (1, ["A2"]),
}
EXPECT_BETA = {
    "Beta Unknown One": (1, ["B1"]),
    "Beta Unknown Two": (1, ["B2"]),
}


def auth(*pairs):
    return [{"name": n, "affiliations": a} for n, a in pairs]


def build_fixture(project):
    """在临时目录里构造双租户本地合作网，返回 (工作目录, 路径字典, CLI 函数)。"""
    work = tempfile.mkdtemp(prefix="rh-tenant-diagnose-")
    landing = os.path.join(work, "harvest.db")
    net = os.path.join(work, "net.db")
    mapping_path = os.path.join(work, "mapping.json")
    with open(mapping_path, "w", encoding="utf-8") as fh:
        json.dump(MAPPING_DOC, fh, ensure_ascii=False, indent=2)

    sys.path.insert(0, project)
    from collector import db as landing_db  # noqa: E402

    conn = landing_db.connect(landing)
    landing_db.init(conn)
    conn.commit()
    conn.close()

    def add(tenant, accession, payload, revision=1):
        conn = landing_db.connect(landing)
        with conn:
            raw = conn.execute(
                "INSERT INTO raw_pages(tenant, scan_id, url, status, body,"
                " fetched_at) VALUES (?,?,?,200,'{}',?)",
                (tenant, f"s-{accession}-r{revision}", f"mem://{accession}",
                 time.time())).lastrowid
            conn.execute(
                "INSERT OR IGNORE INTO result_versions(tenant, accession,"
                " revision, modified_at, retracted, payload, raw_page_id)"
                " VALUES (?,?,?,?,0,?,?)",
                (tenant, accession, revision, time.time(),
                 json.dumps(payload, ensure_ascii=False), raw))
        conn.close()

    # 租户 alpha：名称 1 横跨两篇成果，名称 2 只在 A2；A3 全部可映射
    add("alpha", "A1", {"title": "a1", "authors": auth(
        ("Ann", ["Alpha University", "Alpha Unknown One"]))})
    add("alpha", "A2", {"title": "a2", "authors": auth(
        ("Bob", ["Alpha Unknown One", "Alpha Unknown Two",
                 "Beta Institute"]))})
    add("alpha", "A3", {"title": "a3", "authors": auth(
        ("Cat", ["Alpha Univ", "Beta Institute"]))})
    # 租户 beta：不同的未映射名称、不同的成果编号
    add("beta", "B1", {"title": "b1", "authors": auth(
        ("Dan", ["Beta Unknown One", "Alpha University"]))})
    add("beta", "B2", {"title": "b2", "authors": auth(
        ("Eve", ["Beta Unknown Two", "Beta Institute"]))})

    run_collab = os.path.join(project, "run_collab.py")

    def cli(*args):
        return subprocess.run(
            [sys.executable, run_collab, *args],
            capture_output=True, text=True)

    proc = cli("update", "--db", landing, "--net", net,
               "--mapping", mapping_path)
    if proc.returncode != 0:
        raise RuntimeError("fixture build failed:\n" + proc.stderr)
    return work, {"landing": landing, "net": net,
                  "mapping": mapping_path}, cli


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_json_diagnose(proc):
    return json.loads(proc.stdout)


def load_json_diagnose(proc, label):
    """退出码非 0 或输出不是 JSON 时记一条失败并返回 None，不中断整轮。"""
    if proc.returncode != 0:
        check(False, f"{label}: 退出码 0", proc.stderr.strip()[:160])
        return None
    try:
        return parse_json_diagnose(proc)
    except json.JSONDecodeError:
        check(False, f"{label}: 输出为合法 JSON", proc.stdout.strip()[:160])
        return None


def entries_by_name(doc):
    return {e["name"]: (e["papers"], e["accessions"], e["tenant"])
            for e in doc["unmapped"]}


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="diagnose 租户隔离回归断言（干净副本可运行）")
    ap.add_argument("--project", required=True,
                    help="research_harvester 干净副本的根目录（含 run_collab.py）")
    opts = ap.parse_args(argv)
    project = os.path.abspath(opts.project)
    if not os.path.isfile(os.path.join(project, "run_collab.py")):
        print(f"FATAL: {project} 下找不到 run_collab.py", file=sys.stderr)
        return 1

    work, paths, cli = build_fixture(project)
    net = paths["net"]
    try:
        # ------------------------------------------------------------------
        # 修复前行为取证（仅记录，不参与断言）
        # ------------------------------------------------------------------
        section("取证：当前二进制对 diagnose 的实际响应（修复前后对照用）")
        legacy_bare = cli("diagnose", "--net", net)
        print(f"  不带 --tenant：exit={legacy_bare.returncode}")
        print("  stdout 首行:",
              (legacy_bare.stdout.strip().splitlines() or ["<empty>"])[0][:100])
        legacy_tenant = cli("diagnose", "--net", net,
                            "--tenant", "alpha")
        print(f"  带 --tenant alpha：exit={legacy_tenant.returncode}")
        if legacy_tenant.stderr.strip():
            print("  stderr:", legacy_tenant.stderr.strip().splitlines()[0][:120])

        # ------------------------------------------------------------------
        # R1 文本输出：只含指定租户的名称/成果数/编号，不串另一租户
        # ------------------------------------------------------------------
        section("R1 文本 diagnose --tenant alpha：不串 beta 数据")
        proc = cli("diagnose", "--net", net, "--tenant", "alpha")
        check(proc.returncode == 0, "退出码 0", proc.stderr.strip())
        out_a = proc.stdout
        check("alpha" in out_a, "文本标明所选租户 alpha")
        for name, (papers, accs) in EXPECT_ALPHA.items():
            check(name in out_a, f"列出 alpha 未映射名称 {name!r}")
            check(f"papers={papers}" in out_a,
                  f"{name!r} 报告正确成果数 papers={papers}")
            for acc in accs:
                check(acc in out_a, f"{name!r} 下列出成果编号 {acc}")
        for name in BETA_NAMES:
            check(name not in out_a,
                  f"不出现 beta 的未映射名称 {name!r}", out_a.strip()[:200])
        for acc in BETA_ACCS:
            check(acc not in out_a,
                  f"不出现 beta 的成果编号 {acc}", out_a.strip()[:200])

        section("R2 文本 diagnose --tenant beta：不串 alpha 数据")
        proc = cli("diagnose", "--net", net, "--tenant", "beta")
        check(proc.returncode == 0, "退出码 0", proc.stderr.strip())
        out_b = proc.stdout
        check("beta" in out_b, "文本标明所选租户 beta")
        for name, (papers, accs) in EXPECT_BETA.items():
            check(name in out_b and f"papers={papers}" in out_b,
                  f"列出 {name!r} 且成果数={papers}")
            for acc in accs:
                check(acc in out_b, f"{name!r} 下列出成果编号 {acc}")
        for name in ALPHA_NAMES:
            check(name not in out_b, f"不出现 alpha 的名称 {name!r}")
        for acc in ALPHA_ACCS:
            check(acc not in out_b, f"不出现 alpha 的成果编号 {acc}")

        # ------------------------------------------------------------------
        # R3/R4 JSON 输出：字段、计数、编号、排序、租户标记
        # ------------------------------------------------------------------
        section("R3 JSON diagnose --tenant alpha：结构与编号严格属于 alpha")
        proc = cli("diagnose", "--net", net, "--tenant", "alpha", "--json")
        doc_a = load_json_diagnose(proc, "R3")
        if doc_a is not None:
            check(doc_a.get("tenant") == "alpha",
                  "JSON 顶层标明所选租户 alpha", str(doc_a.get("tenant")))
            got_a = entries_by_name(doc_a)
            check(set(got_a) == set(EXPECT_ALPHA),
                  "JSON 名称集合恰为 alpha 的两个未映射名称",
                  str(sorted(got_a)))
            for name, (papers, accs) in EXPECT_ALPHA.items():
                if name in got_a:
                    p, a, t = got_a[name]
                    check((p, a) == (papers, accs),
                          f"{name!r} 成果数与编号完全一致",
                          f"papers={p} accs={a}")
                    check(t == "alpha", f"{name!r} 条目租户标记为 alpha")
            check([e["name"] for e in doc_a["unmapped"]]
                  == ["Alpha Unknown One", "Alpha Unknown Two"],
                  "按成果数降序、名称升序排列")
            check(all(set(e["accessions"]) <= ALPHA_ACCS
                      for e in doc_a["unmapped"]),
                  "所有编号都属于 alpha 的成果集合")
            check(all(e["tenant"] == "alpha"
                      for e in doc_a["unmapped"]),
                  "不存在任何其他租户的条目")

        section("R4 JSON diagnose --tenant beta：结构与编号严格属于 beta")
        proc = cli("diagnose", "--net", net, "--tenant", "beta", "--json")
        doc_b = load_json_diagnose(proc, "R4")
        if doc_b is not None:
            check(doc_b.get("tenant") == "beta",
                  "JSON 顶层标明所选租户 beta")
            got_b = entries_by_name(doc_b)
            check(set(got_b) == set(EXPECT_BETA),
                  "JSON 名称集合恰为 beta 的两个未映射名称",
                  str(sorted(got_b)))
            for name, (papers, accs) in EXPECT_BETA.items():
                if name in got_b:
                    p, a, t = got_b[name]
                    check((p, a) == (papers, accs),
                          f"{name!r} 成果数与编号完全一致",
                          f"papers={p} accs={a}")
                    check(t == "beta", f"{name!r} 条目租户标记为 beta")
            check(all(set(e["accessions"]) <= BETA_ACCS
                      for e in doc_b["unmapped"]),
                  "所有编号都属于 beta 的成果集合")

        # 文本与 JSON 两种呈现必须相互一致
        section("R5 文本与 JSON 呈现一致")
        for tenant, doc, text, expect in (
                ("alpha", doc_a, out_a, EXPECT_ALPHA),
                ("beta", doc_b, out_b, EXPECT_BETA)):
            if doc is None:
                check(False, f"{tenant}: JSON 缺失，无法与文本对照")
                continue
            for name, (papers, accs) in expect.items():
                check(name in text and f"papers={papers}" in text,
                      f"{tenant}: 文本含 {name!r} 与 papers={papers}")
                for acc in accs:
                    check(acc in text, f"{tenant}: 文本含编号 {acc}")
            check(len(doc["unmapped"]) == len(expect),
                  f"{tenant}: 文本/JSON 名称条目数一致")

        # ------------------------------------------------------------------
        # R6 正常空结果：无数据租户与未知租户 ID 都不得变成全局查询
        # ------------------------------------------------------------------
        section("R6 空结果是正常返回，绝不回退全局查询")
        proc = cli("diagnose", "--net", net, "--tenant", "gamma")
        check(proc.returncode == 0, "无数据租户 gamma 退出码 0")
        check(not any(n in proc.stdout for n in
                      ALPHA_NAMES | BETA_NAMES),
              "gamma 文本结果不含任何租户的未映射名称")
        proc = cli("diagnose", "--net", net, "--tenant", "gamma", "--json")
        doc_g = load_json_diagnose(proc, "R6 gamma --json")
        if doc_g is not None:
            check(doc_g["unmapped"] == [],
                  "gamma JSON 为空列表 unmapped=[]", proc.stdout.strip())
            check(doc_g.get("tenant") == "gamma",
                  "空结果仍标明请求的租户")
        proc = cli("diagnose", "--net", net, "--tenant", "no-such-tenant",
                   "--json")
        doc_n = load_json_diagnose(proc, "R6 unknown-tenant --json")
        if doc_n is not None:
            check(doc_n["unmapped"] == [],
                  "未知租户 ID 同样是空结果而非全局数据")

        # ------------------------------------------------------------------
        # R7 缺参 / 空白值：退出码 2，且 stdout 不得泄露任何数据
        # ------------------------------------------------------------------
        section("R7 缺少 --tenant 与空白租户值都被拒绝（退出码 2）")
        proc = cli("diagnose", "--net", net)
        check(proc.returncode == 2, "缺少 --tenant 被拒绝",
              proc.stderr.strip()[:120])
        check(not proc.stdout.strip(),
              "拒绝时 stdout 不输出任何诊断数据")
        for blank in ("", "   ", "\t"):
            proc = cli("diagnose", "--net", net, "--tenant", blank)
            check(proc.returncode == 2,
                  f"空白租户值 {blank!r} 被拒绝", proc.stderr.strip()[:120])
            check(not proc.stdout.strip(),
                  f"空白值 {blank!r} 不触发任何查询输出")
            check(proc.stderr.strip(), f"空白值 {blank!r} 给出明确错误信息")

        # ------------------------------------------------------------------
        # R8 其他查询命令、网络计算与原始落地库保持原样
        # ------------------------------------------------------------------
        section("R8 其他命令与落地库不受影响")
        before_hash = sha256_file(paths["landing"])
        proc = cli("status", "--net", net, "--json")
        check(proc.returncode == 0, "status 仍可正常运行", proc.stderr.strip())
        status = json.loads(proc.stdout)
        status_tenants = {r["tenant"] for r in status["unmapped_names"]}
        check({"alpha", "beta"} <= status_tenants,
              "status 仍是面向全网文件的汇总（含两个租户）",
              str(status_tenants))
        proc = cli("neighbors", "--net", net, "--tenant", "alpha",
                   "--inst", "INST-A", "--json")
        check(proc.returncode == 0,
              "neighbors 等既有查询命令行为不变", proc.stderr.strip())
        check(json.loads(proc.stdout)["tenant"] == "alpha",
              "neighbors 仍按其既有租户参数工作")
        # 全部 diagnose 调用结束后落地库必须逐字节不变（只读约束）
        check(sha256_file(paths["landing"]) == before_hash,
              "原始落地库在所有 diagnose 查询后逐字节不变")
        # 网络库里两个租户的未映射行都还在（diagnose 只读，不清理数据）
        conn = sqlite3.connect(net)
        try:
            kept = {r[0] for r in conn.execute(
                "SELECT DISTINCT tenant FROM collab_unmapped")}
        finally:
            conn.close()
        check(kept == {"alpha", "beta"},
              "网络库诊断数据未被查询修改或清理", str(kept))

    finally:
        import shutil
        shutil.rmtree(work, ignore_errors=True)

    print(f"\n{'ALL PASSED' if not FAILURES else 'FAILURES'}:"
          f" {len(FAILURES)} 个断言失败")
    if FAILURES:
        for label in FAILURES:
            print(f"  FAILED: {label}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
