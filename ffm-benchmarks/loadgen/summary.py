#!/usr/bin/env python3
"""Prints echo results (JSON lines) as a Markdown table: summary.py results.jsonl"""
import json, sys

print("| server | scenario | rps | p50 us | p99 us | p99.9 us |")
print("|---|---|---:|---:|---:|---:|")
for line in open(sys.argv[1]):
    r = json.loads(line)
    print(f"| {r['server']} | {r['scenario']} | {r['rps']:.0f} | {r['p50_us']} | {r['p99_us']} | {r['p999_us']} |")
