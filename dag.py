import os.path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as t
from pyspark.sql import functions as f

from dagster import op, graph, IOManager, io_manager, repository

COLUMNS = ['title', 'score', 'author', 'total_awards_received', 'date', 'full_link', 'num_comments']


class LocalParquetIOManager(IOManager):

    def _get_path(self, context):
        return os.path.join("temp/", context.run_id, context.step_key, context.name)

    def load_input(self, context) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))

    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))


@io_manager
def local_parquet_io_manager():
    return LocalParquetIOManager()


@op
def make_posts() -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('data/r_dataisbeautiful_posts.csv', header=True, inferSchema=True)
    df_rename = df.withColumnRenamed('created_utc', 'date')
    df_filter = df_rename.filter(df_rename.removed_by.isNull())
    return df_filter.select(*COLUMNS)


@op
def epoch_converter(df: DataFrame):
    return df.withColumn('date', f.from_unixtime('date', 'yyyy-MM-dd HH:mm:ss'))


@op
def cast_columns(df: DataFrame) -> DataFrame:
    return df.withColumn('score', df.score.cast(t.IntegerType())) \
        .withColumn('num_comments', df.num_comments.cast(t.IntegerType())) \
        .withColumn('total_awards_received', df.total_awards_received.cast(t.IntegerType())) \
        .withColumn('date', df.date.cast(t.TimestampType()))


@op
def drop_nulls(df: DataFrame):
    return df.na.drop(subset=['score', 'num_comments'])


@op
def most_comments(df: DataFrame):
    return df.orderBy(['score', 'num_comments'], ascending=[0, 1])


@graph
def make_and_filter_posts():
    most_comments(drop_nulls(cast_columns(epoch_converter(make_posts()))))


make_and_filter_posts_job = make_and_filter_posts.to_job(
    resource_defs={'io_manager': local_parquet_io_manager}  # ResourceDefinition error: <class
    # 'dagster._core.definitions.resource_definition.ResourceDefinition'>. Got value <dag.LocalParquetIOManager
    # object at 0x7fec384a74c0> of type <class 'dag.LocalParquetIOManager'>
)


@repository
def reddit_dagster():
    return [make_and_filter_posts_job]
