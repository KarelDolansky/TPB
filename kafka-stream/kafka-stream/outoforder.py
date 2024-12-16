from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col

# Nastavení prostředí
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env_settings = EnvironmentSettings.in_streaming_mode()
stream_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# Vytvoření zdrojové Kafka tabulky
stream_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('url', DataTypes.STRING())
                .column('title', DataTypes.STRING())
                .column('content', DataTypes.STRING())
                .column('category', DataTypes.STRING())
                .column('photo_count', DataTypes.INT())
                .column('time', DataTypes.TIMESTAMP(3))
                .column('comment_count', DataTypes.INT())
                .column_by_expression("ts", "CAST(NOW() AS TIMESTAMP(3))")
                .watermark("ts", "ts - INTERVAL '3' SECOND")  # Watermark pro ts
                .build())
        .option('topic', 'test-topic')
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('properties.group.id', 'flink-group')
        .option('scan.startup.mode', 'latest-offset')
        .format(FormatDescriptor.for_format('json')
                .option('ignore-parse-errors', 'true')
                .build())
        .build()
)

# Vytvoření dočasné tabulky pro detekci článků mimo pořadí
stream_env.create_temporary_table(
    'out_of_order_sink',
    TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('time', DataTypes.TIMESTAMP(3))
                .column('previous_time', DataTypes.TIMESTAMP(3))
                .build())
        .option('path', '/path/to/out_of_order_articles')  # Upravte cestu podle potřeby
        .format(FormatDescriptor.for_format('csv').build())
        .build()
)

# Dotaz pro detekci článků mimo pořadí
out_of_order_table = stream_env.sql_query("""
    SELECT 
        title, 
        time,
        (SELECT MAX(t2.time) FROM kafka_source AS t2 WHERE t2.time < t1.time) AS previous_time
    FROM 
        kafka_source AS t1
    WHERE 
        time < (SELECT MAX(t2.time) FROM kafka_source AS t2 WHERE t2.time < t1.time)
""")

# Vložení detekovaných článků do sinku
out_of_order_table.execute_insert('out_of_order_sink')

# Spuštění aplikace
stream_env.execute("Kafka Out-of-Order Detection")
