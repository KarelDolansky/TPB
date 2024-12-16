from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor
from pyflink.table.expressions import col, lit
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.window import Slide
from pyflink.table.udf import udtf

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env_settings = EnvironmentSettings.in_streaming_mode()
stream_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

stream_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('url', DataTypes.STRING())
                .column('title', DataTypes.STRING())
                .column('content', DataTypes.STRING())
                .column('category', DataTypes.STRING())
                .column('image_count', DataTypes.INT())
                .column('publication_date', DataTypes.TIMESTAMP(3))
                .column('comment_count', DataTypes.INT())
                .column_by_expression("ts", "CAST(NOW() AS TIMESTAMP(3))")
                .watermark("ts", "ts - INTERVAL '3' SECOND")
                .build())
        .option('topic', 'test-topic')
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('properties.group.id', 'flink-group')
        .option('scan.startup.mode', 'latest-offset')
        .format(FormatDescriptor.for_format('json')
                .option('ignore-parse-errors', 'true')
                .build())
        .build())

pipeline = stream_env.create_statement_set()

data = stream_env.from_path("kafka_source")

stream_env.create_temporary_table(
    'print_sink',
    TableDescriptor.for_connector("print")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comment_count', DataTypes.INT())
                .column('ts', DataTypes.TIMESTAMP())
                .build())
        .build())

titles = data.select(col("title"), col("comment_count"), col("ts"))
pipeline.add_insert("print_sink", titles)

stream_env.create_temporary_table(
    'file_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comment_count', DataTypes.INT())
                .build())
        .option('path', '/files/output.csv')
        .format(FormatDescriptor.for_format("csv")
                .option('field-delimiter', ';')
                .build())
        .build())

filedata = data.filter(col("comment_count") > 100) \
               .select(col("title"), col("comment_count"))
pipeline.add_insert("file_sink", filedata)

# Add sliding window logic
stream_env.create_temporary_table(
    'window_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('start', DataTypes.TIMESTAMP_LTZ())
                .column('end', DataTypes.TIMESTAMP_LTZ())
                .column('article_count', DataTypes.BIGINT())
                .column('war_count', DataTypes.INT())
                .build())
        .option('path', '/files/output/window.csv')
        .format(FormatDescriptor.for_format("csv")
                .option('field-delimiter', ';')
                .build())
        .build())

windowed_data = data.window(Slide.over(lit(1).minute).every(lit(10).second).on(col('ts')).alias('w')) \
    .group_by(col('w')) \
    .select(
        col('w').start.alias('start'),
        col('w').end.alias('end'),
        lit(1).count.alias('article_count'),
        col('content').like('%válka%').cast(DataTypes.INT()).sum.alias('war_count')
    )

pipeline.add_insert("window_sink", windowed_data)

pipeline.execute().wait()
