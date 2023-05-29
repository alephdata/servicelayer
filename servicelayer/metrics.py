from opencensus.ext.stackdriver import stats_exporter
from opencensus.stats import aggregation
from opencensus.stats import measure
from opencensus.stats import stats
from opencensus.stats import view


# A measure that represents task latency in ms.
LATENCY_MS = measure.MeasureFloat(
    "task_latency", "The task latency in milliseconds", "ms"
)

# A view of the task latency measure that aggregates measurements according to
# a histogram with predefined bucket boundaries. This aggregate is periodically
# exported to Stackdriver Monitoring.
LATENCY_VIEW = view.View(
    "task_latency_distribution",
    "The distribution of the task latencies",
    [],
    LATENCY_MS,
    # Latency in buckets: [>=0ms, >=100ms, >=200ms, >=400ms, >=1s, >=2s, >=4s]
    aggregation.DistributionAggregation([100.0, 200.0, 400.0, 1000.0, 2000.0, 4000.0]),
)

MEASURES = [
    LATENCY_MS,
]

VIEWS = [
    LATENCY_VIEW,
]


def init():
    for view in VIEWS:
        stats.stats.view_manager.register_view(view)
    exporter = stats_exporter.new_stats_exporter()
    # print('Exporting stats to project "{}"'.format(exporter.options.project_id))
    stats.stats.view_manager.register_exporter(exporter)


def record_value(measure, value):
    mmap = stats.stats.stats_recorder.new_measurement_map()
    mmap.measure_float_put(measure, value)
    mmap.record()
