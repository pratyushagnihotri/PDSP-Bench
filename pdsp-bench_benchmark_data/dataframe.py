#!/usr/bin/env python3

import os
import json
import statistics
import pandas as pd


def compute_parallelism_avg(par_str: str) -> float:
    """
    Compute the average of the middle parallelism values,
    excluding the first and last numbers.

    Example:
        "10,8,8,1" -> average of [8, 8] = 8.0

    For edge cases with <= 2 numbers, fall back to the
    average of all numbers so that downstream code
    (e.g., get_class) still receives a numeric value.
    """
    nums = [float(v) for v in par_str.split(",")]

    # If there are not enough elements to have "middle" values,
    # fall back to the mean of all elements.
    if len(nums) <= 2:
        return statistics.fmean(nums)

    middle = nums[1:-1]
    return statistics.fmean(middle)


def get_storm_values(info: dict, data: dict, path: list) -> dict:
    output = {}

    output["framework"] = "storm"
    output["query"] = get_query_from_directory(path[1])
    output["parallelism"] = info["job_parallelization"]
    output["parallelism_source"] = output["parallelism"].split(",")[0]
    output["parallelism_avg"] = compute_parallelism_avg(output["parallelism"])
    output["parallelism_sink"] = output["parallelism"].split(",")[-1]
    output["class"], output["class_num"], output["class_num_distinct"] = get_class(output["parallelism_avg"])
    output["producer_rate"] = info["producer_event_per_second"]
    #output["hardware_type"] = info["cluster_hardware_type"] \
    #        if "cluster_hardware_type" in info \
    #        else "-".join(path[2].split("-")[:-1])
    #output["hardware_type"] = "-".join(path[2].split("-")[:-1])
    output["hardware_type"] = path[2]
    if path[2][-1] == "f":
        output["hardware_type"] = output["hardware_type"] + " (mem)"
    output["nodes"] = int(info["cluster_nodes"]) \
            if "cluster_nodes" in info \
            else int(path[2].split("-")[-1])
    output["slots_per_node"] = int(info["cluster_slots_per_node"]) \
            if "cluster_slots_per_node" in info \
            else int(path[3].split("-")[-1]) / int(output["nodes"])
    output["slots"] = output["slots_per_node"] * output["nodes"]
    output["enumeration_strategy"] = info["enumeration_strategy"] \
            if "enumeration_strategy" in info \
            else "Custom"
            #else pd.NA
    output["time"] = info["job_run_time"]
    output["window_size"] = info["job_window_size"]
    output["window_slide"] = info["job_window_slide_size"]

    event_rate_50 = 0
    event_rate_95 = 0
    event_rate_98 = 0

    for key, value in data["queryMetrics"].items():
        if key.startswith("recPerSecOutSource") and key.endswith("50"):
            event_rate_50 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("95"):
            event_rate_95 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("98"):
            event_rate_98 += float(value)

    output["event_rate_50"] = event_rate_50
    output["event_rate_95"] = event_rate_95
    output["event_rate_98"] = event_rate_98

    throughput_50 = 0
    throughput_95 = 0
    throughput_98 = 0

    for key, value in data["queryMetrics"].items():
        if key.startswith("throughput") and key.endswith("50"):
            throughput_50 += float(value)
        elif key.startswith("throughput") and key.endswith("95"):
            throughput_95 += float(value)
        elif key.startswith("throughput") and key.endswith("98"):
            throughput_98 += float(value)

    output["throughput_50"] = throughput_50
    output["throughput_95"] = throughput_95
    output["throughput_98"] = throughput_98

    output["endtoend_latency"] = \
        float(data["queryMetrics"]["endtoend_latency_storm"])

    output = output | get_hardware_values(info, data)

    return pd.DataFrame([output])


def get_flink_values(info: dict, data: dict, path: list) -> dict:
    output = {}

    output["framework"] = "flink"
    output["query"] = get_query_from_directory(path[1])
    output["parallelism"] = info["job_parallelization"]
    output["parallelism_source"] = output["parallelism"].split(",")[0]
    output["parallelism_avg"] = compute_parallelism_avg(output["parallelism"])
    output["parallelism_sink"] = output["parallelism"].split(",")[-1]
    output["class"], output["class_num"], output["class_num_distinct"] = get_class(output["parallelism_avg"])
    output["producer_rate"] = info["producer_event_per_second"]
    #output["hardware_type"] = info["cluster_hardware_type"] \
    #        if "cluster_hardware_type" in info \
    #        else "-".join(path[2].split("-")[:-1])
    #output["hardware_type"] = "-".join(path[2].split("-")[:-1])
    output["hardware_type"] = path[2]
    if path[2][-1] == "f":
        output["hardware_type"] = output["hardware_type"] + " (mem)"
    output["nodes"] = int(info["cluster_nodes"]) \
            if "cluster_nodes" in info \
            else int(path[2].split("-")[-1])
    output["slots_per_node"] = int(info["cluster_slots_per_node"]) \
            if "cluster_slots_per_node" in info \
            else int(path[3].split("-")[-1]) / int(output["nodes"])
    output["slots"] = output["slots_per_node"] * output["nodes"]
    output["enumeration_strategy"] = info["enumeration_strategy"] \
            if "enumeration_strategy" in info \
            else "Custom"
            #else pd.NA
    output["time"] = info["job_run_time"]
    output["window_size"] = info["job_window_size"]
    output["window_slide"] = info["job_window_slide_size"]

    event_rate_50 = 0
    event_rate_95 = 0
    event_rate_98 = 0
    for key, value in data["queryMetrics"].items():
        if key.startswith("recPerSecOutSource") and key.endswith("50"):
            event_rate_50 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("95"):
            event_rate_95 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("98"):
            event_rate_98 += float(value)
    output["event_rate_50"] = event_rate_50
    output["event_rate_95"] = event_rate_95
    output["event_rate_98"] = event_rate_98

    throughput_50 = 0
    throughput_95 = 0
    throughput_98 = 0
    for key, value in data["queryMetrics"].items():
        if key.startswith("throughput") and key.endswith("50"):
            throughput_50 += float(value)
        elif key.startswith("throughput") and key.endswith("95"):
            throughput_95 += float(value)
        elif key.startswith("throughput") and key.endswith("98"):
            throughput_98 += float(value)
    output["throughput_50"] = throughput_50
    output["throughput_95"] = throughput_95
    output["throughput_98"] = throughput_98

    latency = 0
    for key, value in data["queryMetrics"].items():
        if key.startswith("endtoend_latency") and key.endswith("50"):
            latency += float(value)
    output["endtoend_latency"] = latency

    output = output | get_hardware_values(info, data)

    return pd.DataFrame([output])


def get_class(n: int) -> str:
    if n <= 5:
        return ("XS", 1, 1)
    elif n <= 10:
        return ("S", 2, 2)
    elif n <= 15:
        return ("S", 2, 3)
    elif n <= 20:
        return ("M", 3, 4)
    elif n <= 25:
        return ("M", 3, 5)
    elif n <= 30:
        return ("L", 4, 6)
    elif n <= 35:
        return ("L", 4, 7)
    elif n <= 40:
        return ("XL", 5, 8)
    elif n <= 45:
        return ("XL", 5, 9)
    elif n <= 50:
        return ("XL", 5, 10)
    elif n <= 55:
        return ("XL", 5, 11)
    elif n <= 60:
        return ("XXL", 6, 12)
    elif n <= 65:
        return ("XXL", 6, 13)
    elif n <= 70:
        return ("XXL", 6, 14)
    elif n <= 75:
        return ("XXL", 6, 15)
    elif n <= 80:
        return ("XXL", 6, 16)
    else:
        return ("XXL", 6, 16)

def get_query_from_directory(directory: str) -> str:
    if "word-count" == directory:
        return "Word Count"
    if "word-count-shuffle" == directory:
        return "Word Count Shuffle"
    elif "smart-grid" == directory:
        return "Smart Grid"
    elif "ad-analytics" == directory:
        return "Ad Analytics"
    elif "google-cloud-monitoring" == directory:
        return "Google Cloud Monitoring"
    elif "sentiment-analysis" == directory:
        return "Sentiment Analysis"
    elif "spike-detection" == directory:
        return "Spike Detection"
    elif "log-processing" == directory:
        return "Log Processing"
    elif "trending-topics" == directory:
        return "Trending Topics"
    elif "bargain-index" == directory:
        return "Bargain Index"
    elif "click-analytics" == directory:
        return "Click Analytics"
    elif "click-analytics-1" == directory:
        return "Click Analytics"
    elif "click-analytics-2" == directory:
        return "Click Analytics Count-based"
    elif "machine-outlier" == directory:
        return "Machine Outlier"
    elif "linear-road" == directory:
        return "Linear Road"
    elif "tpch" == directory:
        return "TPCH"
    elif "tpch-1" == directory:
        return "TPCH"
    elif "tpch-2" == directory:
        return "TPCH Window"
    elif "traffic-monitoring" == directory:
        return "Traffic Monitoring"
    else:
        raise NotImplementedError("Directory \"" + directory + "\" is not assigned a query name.")


def get_hardware_values(info: dict, data: dict) -> dict:
    output = {}

    cpu_50 = 0
    cpu_95 = 0
    cpu_98 = 0
    for key, value in data["hardwareMetrics"].items():
        if key == "node_cpu_50":
            cpu_50 += float(value)
        elif key == "node_cpu_95":
            cpu_95 += float(value)
        elif key == "node_cpu_98":
            cpu_98 += float(value)
    output["cpu_50"] = cpu_50
    output["cpu_95"] = cpu_95
    output["cpu_98"] = cpu_98

    memory_50 = 0
    memory_95 = 0
    memory_98 = 0
    for key, value in data["hardwareMetrics"].items():
        if key == "node_memory_50":
            memory_50 += float(value)
        elif key == "node_memory_95":
            memory_95 += float(value)
        elif key == "node_memory_98":
            memory_98 += float(value)
    output["memory_50"] = memory_50
    output["memory_95"] = memory_95
    output["memory_98"] = memory_98

    network_50 = 0
    network_95 = 0
    network_98 = 0
    for key, value in data["hardwareMetrics"].items():
        if key == "node_network_out_50":
            network_50 += float(value)
        elif key == "node_network_out_95":
            network_95 += float(value)
        elif key == "node_network_out_98":
            network_98 += float(value)
    output["network_50"] = network_50
    output["network_95"] = network_95
    output["network_98"] = network_98

    return output


def get_artifacts() -> list[str]:
    for root, dirs, files in os.walk("artifacts/"):
        for file in files:
            if file.startswith("evaluationMetrics"):
                yield (root, file
                       .removeprefix("evaluationMetrics-")
                       .removesuffix(".json"))


def main():
    df = pd.DataFrame()

    for path, artifact in get_artifacts():
        print(os.path.join(path, artifact))
        info = {}
        data = {}
        with open(os.path.join(path, "metaInfo-" + artifact +
                               ".json")) as f:
            info = json.load(f)
        with open(os.path.join(path, "evaluationMetrics-" + artifact +
                               ".json")) as f:
            data = json.load(f)

        output = {}
        if info["job_framework"] == "Apache Storm":
            output = get_storm_values(info, data, path.split("/"))
        elif info["job_framework"] == "Apache Flink":
            output = get_flink_values(info, data, path.split("/"))
        output["path"] = path
        output["artifact"] = artifact
        output["count"] = 1
        df = pd.concat([df, output], ignore_index=True)

    df = df.sort_index()
    df.to_csv('data.csv', index=False)


if __name__ == '__main__':
    main()
