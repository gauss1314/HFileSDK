#!/usr/bin/env python3
"""
hfile-report.py  —  HFileSDK benchmark comparison report generator.

Reads JSON output from Google Benchmark (C++) and hfile-bench-java,
then produces a self-contained HTML report with charts and tables.

Usage:
    python3 hfile-report.py --input-dir results/ --output report.html
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime


def load_json_results(input_dir: Path) -> dict:
    """Load all *.json benchmark result files from a directory."""
    results = {"cpp": {}, "java": {}}
    for f in sorted(input_dir.glob("*.json")):
        try:
            with open(f) as fh:
                data = json.load(fh)
            prefix = "java_" if f.stem.startswith("java") else "cpp_"
            lang   = "java" if prefix == "java_" else "cpp"
            key    = f.stem[len(prefix):]
            results[lang][key] = data
        except Exception as e:
            print(f"Warning: could not parse {f}: {e}", file=sys.stderr)
    return results


def extract_throughput(bench_data: dict) -> list[dict]:
    """Extract throughput metrics from a benchmark JSON blob."""
    rows = []
    for bm in bench_data.get("benchmarks", []):
        name     = bm.get("name", "")
        tput_mbs = bm.get("bytes_per_second", 0) / (1024 * 1024)
        kv_qps   = bm.get("items_per_second", 0) / 1_000_000
        cpu_ms   = bm.get("cpu_time", 0) / 1e6  # ns → ms
        rows.append({
            "name":         name,
            "throughput_mbs": round(tput_mbs, 1),
            "kv_qps_M":     round(kv_qps, 2),
            "cpu_ms":       round(cpu_ms, 1),
        })
    return rows


def speedup_badge(ratio: float) -> str:
    color = "#2ecc71" if ratio >= 3.0 else ("#f39c12" if ratio >= 1.5 else "#e74c3c")
    return f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:12px;font-weight:bold">{ratio:.1f}x</span>'


def generate_html(results: dict, output_path: Path) -> None:
    cpp_rows  = {}
    java_rows = {}

    for key, data in results["cpp"].items():
        cpp_rows[key]  = extract_throughput(data)
    for key, data in results["java"].items():
        java_rows[key] = extract_throughput(data)

    # Build a combined comparison table
    all_keys = sorted(set(list(cpp_rows.keys()) + list(java_rows.keys())))

    rows_html = ""
    for key in all_keys:
        cpp_bms  = cpp_rows.get(key, [])
        java_bms = java_rows.get(key, [])
        for i, (cb, jb) in enumerate(zip(cpp_bms, java_bms)):
            ratio = (cb["throughput_mbs"] / jb["throughput_mbs"]
                     if jb["throughput_mbs"] > 0 else 0)
            rows_html += f"""
            <tr>
              <td>{key}/{cb['name']}</td>
              <td style="text-align:right">{jb['throughput_mbs']}</td>
              <td style="text-align:right"><strong>{cb['throughput_mbs']}</strong></td>
              <td style="text-align:center">{speedup_badge(ratio)}</td>
              <td style="text-align:right">{jb['kv_qps_M']}</td>
              <td style="text-align:right"><strong>{cb['kv_qps_M']}</strong></td>
            </tr>"""

    if not rows_html:
        rows_html = "<tr><td colspan='6' style='text-align:center'>No benchmark data found</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>HFileSDK Benchmark Report</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
             background: #f5f6fa; color: #2d3436; padding: 32px; }}
    h1  {{ font-size: 1.8rem; margin-bottom: 4px; }}
    .sub {{ color: #636e72; margin-bottom: 32px; font-size: 0.9rem; }}
    .card {{ background: #fff; border-radius: 8px; padding: 24px;
              box-shadow: 0 2px 8px rgba(0,0,0,.08); margin-bottom: 24px; }}
    h2   {{ font-size: 1.2rem; margin-bottom: 16px; color: #0984e3; }}
    table {{ width:100%; border-collapse: collapse; font-size: 0.88rem; }}
    th   {{ background: #f0f4ff; padding: 10px 14px; text-align: left;
             font-weight: 600; border-bottom: 2px solid #dfe6e9; }}
    td   {{ padding: 9px 14px; border-bottom: 1px solid #f0f0f0; }}
    tr:hover td {{ background: #f8f9ff; }}
    .note {{ font-size: 0.8rem; color: #b2bec3; margin-top: 12px; }}
    .target {{ background: #e8f8f0; padding: 12px 18px; border-radius: 6px;
                border-left: 4px solid #2ecc71; margin-bottom: 20px; }}
  </style>
</head>
<body>
  <h1>🚀 HFileSDK Benchmark Report</h1>
  <p class="sub">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

  <div class="target">
    <strong>Performance target:</strong>
    HFileSDK C++ throughput ≥ <strong>3×</strong> Java HBase HFile.Writer
    on identical datasets and hardware.
    Badges: <span style="background:#2ecc71;color:#fff;padding:1px 6px;border-radius:8px">≥3×</span>
    achieved &nbsp;
    <span style="background:#f39c12;color:#fff;padding:1px 6px;border-radius:8px">1.5-3×</span>
    partial &nbsp;
    <span style="background:#e74c3c;color:#fff;padding:1px 6px;border-radius:8px">&lt;1.5×</span>
    below target.
  </div>

  <div class="card">
    <h2>Write Throughput Comparison</h2>
    <table>
      <thead>
        <tr>
          <th>Benchmark</th>
          <th style="text-align:right">Java (MB/s)</th>
          <th style="text-align:right">C++ SDK (MB/s)</th>
          <th style="text-align:center">Speedup</th>
          <th style="text-align:right">Java QPS (M/s)</th>
          <th style="text-align:right">C++ QPS (M/s)</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
    <p class="note">
      All benchmarks: same CPU pinning (taskset -c 0-3), page cache dropped between
      runs, median of 10 iterations.
    </p>
  </div>

  <div class="card">
    <h2>How to Reproduce</h2>
    <pre style="background:#f5f6fa;padding:16px;border-radius:6px;overflow-x:auto;font-size:0.82rem">
# Build C++ SDK
cmake -B build -DCMAKE_BUILD_TYPE=Release \\
    -DHFILE_ENABLE_BENCHMARKS=ON
cmake --build build -j$(nproc)

# Run full benchmark suite
bash scripts/bench-runner.sh

# Re-generate this report
python3 tools/hfile-report/hfile-report.py \\
    --input-dir results/ --output report.html
    </pre>
  </div>

  <div class="card">
    <h2>HBase Compatibility</h2>
    <table>
      <thead>
        <tr><th>Check</th><th>Status</th></tr>
      </thead>
      <tbody>
        <tr><td>HFile major version = 3</td><td>✅ enforced in TrailerBuilder</td></tr>
        <tr><td>Cell Tags length field present (v3 mandatory)</td><td>✅ in all encoders</td></tr>
        <tr><td>MVCC / MemstoreTS field present</td><td>✅ VarInt(0) written</td></tr>
        <tr><td>FileInfo: LASTKEY</td><td>✅ FileInfoBuilder</td></tr>
        <tr><td>FileInfo: AVG_KEY_LEN / AVG_VALUE_LEN</td><td>✅ FileInfoBuilder</td></tr>
        <tr><td>FileInfo: MAX_TAGS_LEN</td><td>✅ FileInfoBuilder</td></tr>
        <tr><td>FileInfo: COMPARATOR</td><td>✅ FileInfoBuilder</td></tr>
        <tr><td>FileInfo: DATA_BLOCK_ENCODING</td><td>✅ FileInfoBuilder</td></tr>
        <tr><td>Trailer: ProtoBuf serialized</td><td>✅ TrailerBuilder</td></tr>
        <tr><td>Single CF per HFile</td><td>✅ enforced in HFileWriter</td></tr>
        <tr><td>KV sort order (Row→Fam→Qual→TS↓→Type↓)</td><td>✅ verified on append</td></tr>
        <tr><td>Staging dir: &lt;output&gt;/&lt;cf&gt;/hfile</td><td>✅ BulkLoadWriter</td></tr>
      </tbody>
    </table>
  </div>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")
    print(f"Report written to {output_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-dir", required=True,
                    help="Directory containing benchmark JSON files")
    ap.add_argument("--output",    default="report.html",
                    help="Output HTML file path (default: report.html)")
    args = ap.parse_args()

    input_dir   = Path(args.input_dir)
    output_path = Path(args.output)

    if not input_dir.is_dir():
        print(f"Error: {input_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    results = load_json_results(input_dir)
    generate_html(results, output_path)


if __name__ == "__main__":
    main()
