"""
解析数据，将metrics 通过 pushgateway 推送到prometheus

"""
from YCSB_collecter.src.resolve_log_file import YCSBResolver
from prometheus_client import push_to_gateway, CollectorRegistry, Summary


class MetricsResolver(object):

    @classmethod
    def judge_workload_type(cls):
        """
        根据READ/UPDATE 等的count判断属于哪个workload类型

        :return:
        """
        return 'workloada'

    @classmethod
    def generate_prometheus_metrics(cls, filename):
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
        ycsb_summary_request_count = Summary('ycsb_summary_request_count', 'YCSB request count',
                                             ['workload', 'type', 'operation_count'], registry=registry)
        ycsb_summary_request_count.labels('workloada', types[0], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[0]['Count']])
        ycsb_summary_request_count.labels('workloada', types[1], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[1]['Count']])

        # 生成ops metrics
        ycsb_summary_request_ops = Summary('ycsb_summary_request_ops', 'YCSB request ops',
                                           ['workload', 'type', 'operation_count'], registry=registry)
        ycsb_summary_request_ops.labels('workloada', types[0], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[0]['OPS']])
        ycsb_summary_request_ops.labels('workloada', types[1], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[1]['OPS']])

        # 生成 Avg(us) metrics
        ycsb_summary_request_ops = Summary('ycsb_summary_request_latency_avg', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count'], registry=registry)
        ycsb_summary_request_ops.labels('workloada', types[0], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[0]['Avg(us)']])
        ycsb_summary_request_ops.labels('workloada', types[1], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[1]['Avg(us)']])

        # 生成 99th(us)
        ycsb_summary_request_ops = Summary('ycsb_summary_request_latency_avg_99th', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count'], registry=registry)
        ycsb_summary_request_ops.labels('workloada', types[0], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[0]['99th(us)']])
        ycsb_summary_request_ops.labels('workloada', types[1], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[1]['99th(us)']])

        # 生成 99.9th(us)
        ycsb_summary_request_ops = Summary('ycsb_summary_request_latency_avg_99.9th', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count'], registry=registry)
        ycsb_summary_request_ops.labels('workloada', types[0], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[0]['99.9th(us)']])
        ycsb_summary_request_ops.labels('workloada', types[1], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[1]['99.9th(us)']])

        # 生成 99.99th(us)
        ycsb_summary_request_ops = Summary('ycsb_summary_request_latency_avg_99.99th', 'YCSB request latency avg',
                                           ['workload', 'type', 'operation_count'], registry=registry)
        ycsb_summary_request_ops.labels('workloada', types[0], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[0]['99.99th']])
        ycsb_summary_request_ops.labels('workloada', types[1], ycsb_elements['operationcount']). \
            observe(ycsb_results[types[1]['99.99th']])

        # push 到 gateway
        push_to_gateway('127.0.0.1:2023', job='batchA', registry=registry)


