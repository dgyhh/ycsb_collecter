import sys
from src.metrics_to_prometheus import MetricsResolver


def run(filepath, workload_type, pushgateway_host):
    MetricsResolver.generate_prometheus_metrics(filepath, workload_type, pushgateway_host)


if __name__ == "__main__":
    args = sys.argv
    # 检查输入参数
    if len(args) <= 1:
        raise Exception('please input parameters')

    param_dict = {item.split('=')[0]: item.split('=')[1] for item in args[1:]}
    if not param_dict.get('filepath'):
        raise Exception('parameter filepath is needed!')
    if not param_dict.get('pushgateway_host'):
        raise Exception('parameter pushgateway_host is needed!')
    run(param_dict.get('filepath'), param_dict.get('workload_type', 'workloada'), param_dict.get('pushgateway_host'))


