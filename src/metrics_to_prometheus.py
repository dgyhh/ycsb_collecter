"""
组织成metrics，通过 pushgateway 推送到prometheus
"""
from src.resolve_log_file import YCSBResolver
from prometheus_client import push_to_gateway, CollectorRegistry, Gauge


class MetricsResolver(object):

    @classmethod
    def generate_prometheus_metrics(cls, filename, workload_type, pushgateway_host):
        """
        以Summary方式生成metrics，通过pushgateway推送到prometheus

        :return:
        """
        ycsb_elements = YCSBResolver(filename).resolve_elements()
        ycsb_results = YCSBResolver(filename).resolve_results()
        # type = READ/UPDATE/INSERT
        types = list(ycsb_results.keys())
        registry = CollectorRegistry()

        # 生成count metrics
        ycsb_gauge_request_count = Gauge('ycsb_gauge_request_count', 'YCSB request count',
                                        ['workload', 'type', 'operation_count', 'thread_count', 'count'], registry=registry)
        ycsb_gauge_request_count.labels(workload_type, types[0], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[0]]['Count'])). \
            set(int(ycsb_results[types[0]]['Count']))
        ycsb_gauge_request_count.labels(workload_type, types[1], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[1]]['Count'])). \
            set(int(ycsb_results[types[1]]['Count']))

        # 生成ops metrics
        ycsb_gauge_request_ops = Gauge('ycsb_gauge_request_ops', 'YCSB request ops',
                                           ['workload', 'type', 'operation_count', 'thread_count', 'count'], registry=registry)
        ycsb_gauge_request_ops.labels(workload_type, types[0], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[0]]['Count'])). \
            set(float(ycsb_results[types[0]]['OPS']))
        ycsb_gauge_request_ops.labels(workload_type, types[1], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[1]]['Count'])). \
            set(float(ycsb_results[types[1]]['OPS']))

        # 生成 Avg(us) metrics
        ycsb_gauge_request_latency_avg = Gauge('ycsb_gauge_request_latency_avg', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count', 'thread_count', 'count'], registry=registry)
        ycsb_gauge_request_latency_avg.labels(workload_type, types[0], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[0]]['Count'])). \
            set(int(ycsb_results[types[0]]['Avg(us)']))
        ycsb_gauge_request_latency_avg.labels(workload_type, types[1], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[1]]['Count'])). \
            set(int(ycsb_results[types[1]]['Avg(us)']))

        # 生成 99th(us) metrics
        ycsb_gauge_request_latency_avg_99th = Gauge('ycsb_gauge_request_latency_avg_99th', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count', 'thread_count', 'count'], registry=registry)
        ycsb_gauge_request_latency_avg_99th.labels(workload_type, types[0], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[0]]['Count'])). \
            set(int(ycsb_results[types[0]]['99th(us)']))
        ycsb_gauge_request_latency_avg_99th.labels(workload_type, types[1], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[1]]['Count'])). \
            set(int(ycsb_results[types[1]]['99th(us)']))

        # 生成 99.9th(us) metrics
        ycsb_gauge_request_latency_avg_999th = Gauge('ycsb_gauge_request_latency_avg_999th', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count', 'thread_count', 'count'], registry=registry)
        ycsb_gauge_request_latency_avg_999th.labels(workload_type, types[0], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[0]]['Count'])). \
            set(int(ycsb_results[types[0]]['99.9th(us)']))
        ycsb_gauge_request_latency_avg_999th.labels(workload_type, types[1], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[1]]['Count'])). \
            set(int(ycsb_results[types[1]]['99.9th(us)']))

        # 生成 99.99th(us) metrics
        ycsb_gauge_request_latency_avg_9999th = Gauge('ycsb_gauge_request_latency_avg_9999th', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count', 'thread_count', 'count'], registry=registry)
        ycsb_gauge_request_latency_avg_9999th.labels(workload_type, types[0], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[0]]['Count'])). \
            set(int(ycsb_results[types[0]]['99.99th(us)']))
        ycsb_gauge_request_latency_avg_9999th.labels(workload_type, types[1], ycsb_elements['threadcount'], ycsb_elements['operationcount'], int(ycsb_results[types[1]]['Count'])). \
            set(int(ycsb_results[types[1]]['99.99th(us)']))

        # push 到 gateway。需设置grouping_key，且grouping_key包含job
        push_to_gateway(pushgateway_host, job='ycsb-collecter', registry=registry,
                        grouping_key={'job': 'ycsb-collecter', 'workload': workload_type, 'thread_count': ycsb_elements['threadcount'],
                                      'operation_count': ycsb_elements['operationcount']})
