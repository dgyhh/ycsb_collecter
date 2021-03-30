from YCSB_collecter.src.metrics_to_prometheus import MetricsResolver
filename = input("input file path: ")


def run(filename):
    MetricsResolver.generate_prometheus_metrics(filename)
