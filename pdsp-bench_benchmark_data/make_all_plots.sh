#!/usr/bin/env bash
set -euo pipefail

CSV="data.csv"
OUT_DIR="plots"

# ---- EDIT THESE ARRAYS TO MATCH YOUR SETUP ----

# All queries (14 apps) – adjust names to match your CSV exactly
QUERIES=(
  "Ad Analytics"
  "Bargain Index"
  "Click Analytics"
  "Google Cloud Monitoring"
  "Linear Road"
  "Log Processing"
  "Machine Outlier"
  "Sentiment Analysis"
  "Smart Grid"
  "Spike Detection"
  "TPCH"
  "Traffic Monitoring"
  "Trending Topics"
  "Word Count"
)

# All hardware types you have
HARDWARES=(
  "c6525-100g"
  "c6525-25g"
  "m510"
)

# All producer rates (event rates) you used
PRODUCER_RATES=(
  500000
  1000000
)

# Metrics you really care about for MOST plots (you can extend)
CORE_METRICS=(
  throughput_50
  endtoend_latency
  cpu_50
  memory_50
  network_50
)

# Full metric list for the big batch
ALL_METRICS=(
  event_rate_50 event_rate_95 event_rate_98
  throughput_50 throughput_95 throughput_98
  endtoend_latency
  cpu_50 cpu_95 cpu_98
  memory_50 memory_95 memory_98
  network_50 network_95 network_98
)

FRAMEWORKS=("flink" "storm")

# ------------------------------------------------
mkdir -p "${OUT_DIR}"

# Helper: safe name from query (spaces -> _)
safe() {
  echo "$1" | tr ' ' '_'
}

# ============================================================
# 1) Per-framework, all queries in one plot (queries_single_framework)
# ============================================================
echo ">>> queries_single_framework ..."
for HW in "${HARDWARES[@]}"; do
  for RATE in "${PRODUCER_RATES[@]}"; do
    for FW in "${FRAMEWORKS[@]}"; do
      for METRIC in "${CORE_METRICS[@]}"; do
        SAVE_BASE="${OUT_DIR}/queries_single/${FW}/${HW}/${RATE}_$(safe "${METRIC}")"
        echo "  ${FW} | ${HW} | rate=${RATE} | metric=${METRIC}"
        python plot_pdsp.py \
          --csv "${CSV}" \
          --plot queries_single_framework \
          --framework "${FW}" \
          --hardware_type "${HW}" \
          --producer_rate "${RATE}" \
          --metric "${METRIC}" \
          --agg mean \
          --save "${SAVE_BASE}" \
          --all-formats \
          --no-show || true
      done
    done
  done
done

# ============================================================
# 2) All queries in one plot, Flink vs Storm (queries_compare_frameworks)
# ============================================================
echo ">>> queries_compare_frameworks ..."
for HW in "${HARDWARES[@]}"; do
  for RATE in "${PRODUCER_RATES[@]}"; do
    for METRIC in "${CORE_METRICS[@]}"; do
      SAVE_BASE="${OUT_DIR}/queries_compare_fw/${HW}/${RATE}_$(safe "${METRIC}")"
      echo "  FW compare | ${HW} | rate=${RATE} | metric=${METRIC}"
      python plot_pdsp.py \
        --csv "${CSV}" \
        --plot queries_compare_frameworks \
        --frameworks "${FRAMEWORKS[@]}" \
        --hardware_type "${HW}" \
        --producer_rate "${RATE}" \
        --metric "${METRIC}" \
        --agg mean \
        --save "${SAVE_BASE}" \
        --all-formats \
        --no-show || true
    done
  done
done

# ============================================================
# 3) Per-query internal scaling (scaling_single)
# ============================================================
echo ">>> scaling_single (internal framework scaling) ..."
for Q in "${QUERIES[@]}"; do
  Q_SAFE=$(safe "${Q}")
  for HW in "${HARDWARES[@]}"; do
    for RATE in "${PRODUCER_RATES[@]}"; do
      for FW in "${FRAMEWORKS[@]}"; do
        for METRIC in "${CORE_METRICS[@]}"; do
          SAVE_BASE="${OUT_DIR}/scaling_single/${Q_SAFE}/${FW}/${HW}/${RATE}_$(safe "${METRIC}")"
          echo "  ${Q} | ${FW} | ${HW} | rate=${RATE} | metric=${METRIC}"
          python plot_pdsp.py \
            --csv "${CSV}" \
            --plot scaling_single \
            --framework "${FW}" \
            --query "${Q}" \
            --hardware_type "${HW}" \
            --producer_rate "${RATE}" \
            --metric "${METRIC}" \
            --agg mean \
            --save "${SAVE_BASE}" \
            --all-formats \
            --no-show || true
        done
      done
    done
  done
done

# ============================================================
# 4) Per-query framework comparison vs parallelism (scaling_compare_fw)
# ============================================================
echo ">>> scaling_compare_fw (Flink vs Storm per query) ..."
for Q in "${QUERIES[@]}"; do
  Q_SAFE=$(safe "${Q}")
  for HW in "${HARDWARES[@]}"; do
    for RATE in "${PRODUCER_RATES[@]}"; do
      for METRIC in "${CORE_METRICS[@]}"; do
        SAVE_BASE="${OUT_DIR}/scaling_compare_fw/${Q_SAFE}/${HW}/${RATE}_$(safe "${METRIC}")"
        echo "  ${Q} | FW compare | ${HW} | rate=${RATE} | metric=${METRIC}"
        python plot_pdsp.py \
          --csv "${CSV}" \
          --plot scaling_compare_fw \
          --frameworks "${FRAMEWORKS[@]}" \
          --query "${Q}" \
          --hardware_type "${HW}" \
          --producer_rate "${RATE}" \
          --metric "${METRIC}" \
          --agg mean \
          --save "${SAVE_BASE}" \
          --all-formats \
          --no-show || true
      done
    done
  done
done

# ============================================================
# 5) For each query+hardware: Flink vs Storm, all event rates (one metric)
#    (batch_compare_eventrates)
# ============================================================
echo ">>> batch_compare_eventrates (per query+hw, all rates, one metric) ..."
# choose one or two representative metrics for these batch plots
BATCH_METRICS=(throughput_50 endtoend_latency)

for Q in "${QUERIES[@]}"; do
  Q_SAFE=$(safe "${Q}")
  for HW in "${HARDWARES[@]}"; do
    for METRIC in "${BATCH_METRICS[@]}"; do
      echo "  ${Q} | ${HW} | metric=${METRIC} | all rates"
      python plot_pdsp.py \
        --csv "${CSV}" \
        --plot batch_compare_eventrates \
        --frameworks "${FRAMEWORKS[@]}" \
        --query "${Q}" \
        --hardware_type "${HW}" \
        --metric "${METRIC}" \
        --agg mean \
        --out_dir "${OUT_DIR}" \
        --all-formats \
        --no-show || true
    done
  done
done

# ============================================================
# 6) For each query+hardware: all metrics × all event rates (big batch)
#    (batch_all_metrics_eventrates)
# ============================================================
echo ">>> batch_all_metrics_eventrates (BIG batch, all metrics × all rates) ..."
for Q in "${QUERIES[@]}"; do
  for HW in "${HARDWARES[@]}"; do
    echo "  ${Q} | ${HW} | ALL metrics, ALL rates"
    python plot_pdsp.py \
      --csv "${CSV}" \
      --plot batch_all_metrics_eventrates \
      --frameworks "${FRAMEWORKS[@]}" \
      --query "${Q}" \
      --hardware_type "${HW}" \
      --metric throughput_50 \
      --agg mean \
      --out_dir "${OUT_DIR}" \
      --all-formats \
      --no-show || true
  done
done

echo ">>> Done. Plots written under '${OUT_DIR}'"
