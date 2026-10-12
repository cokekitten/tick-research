#!/bin/bash
# tick-research 采集状态（在东京采集机上执行）
cd /root/tick-research || exit 1

now=$(date -u +%FT%TZ)
echo "TIME $now"

# 采集进程与速率
for f in data/logs/binance-perp.log data/logs/binance-spot.log data/logs/bybit-perp.log data/logs/bybit-spot.log data/logs/okx-perp.log data/logs/okx-spot.log; do
  [ -f "$f" ] || continue
  name=$(basename "$f" .log)
  line=$(grep '^\[' "$f" | tail -1)
  echo "RATE $name $line"
done

# 进程存活
procs=$(pgrep -fc "tick.collect" 2>/dev/null || echo 0)
echo "PROCS collectors=$procs"

# 本地磁盘与待传数据
free=$(df -h / | awk 'NR==2{print $4}')
pending=$(find data/raw -name "*.jsonl.zst" 2>/dev/null | wc -l | tr -d ' ')
partial=$(find data/raw -name "*.partial" 2>/dev/null | wc -l | tr -d ' ')
rawsize=$(du -sh data/raw 2>/dev/null | cut -f1)
echo "DISK free=$free raw=$rawsize sealed_pending=$pending active_partial=$partial"

# 上传情况（最近一次 pass + 错误）
if [ -f logs/upload.log ]; then
  echo "UPLOAD_LAST $(grep 'upload pass done' logs/upload.log | tail -1)"
  err=$(grep -ciE "error|failed|critical" logs/upload.log 2>/dev/null || echo 0)
  echo "UPLOAD_ERRORS total=$err"
fi

# 云端已有多少
cloud=$(rclone size gdrive:tick-research/raw 2>/dev/null | tr '\n' ' ')
echo "CLOUD $cloud"

# 停机/限速标记
for s in data/meta/STOP-* ; do [ -e "$s" ] && echo "ALERT stopfile=$s $(tail -1 $s 2>/dev/null)"; done
echo "END"
