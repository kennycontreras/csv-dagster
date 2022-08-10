import os.path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as t
from pyspark.sql import functions as f

from dagster import op, graph, IOManager, io_manager, repository

COLUMNS = ['title', 'score', 'author', 'total_awards_received', 'date', 'full_link', 'num_comments']


class LocalParquetIOManager(IOManager):

    @staticmethod
    def _get_path(context):
        return os.path.join(context.run_config, context.step_key, context.name)

    def load_input(self, context) -> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))

    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))


@io_manager
def local_io_manager():
    return LocalParquetIOManager()


@op
def make_posts() -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('data/r_dataisbeautiful_posts.csv', header=True, inferSchema=True)
    df_rename = df.withColumnRenamed('created_utc', 'date')
    df_filter = df_rename.filter(df_rename.removed_by.isNull())
    df_select = df_filter.select(*COLUMNS)
    df_cast = df_select.withColumn('score', df_select.score.cast(t.IntegerType()))\
        .withColumn('num_comments', df_select.num_comments.cast(t.IntegerType()))\
        .withColumn('total_awards_received', df_select.total_awards_received.cast(t.IntegerType()))\
        .withColumn('date', df_select.date.cast(t.TimestampType()))
    return df_cast


@op
def epoch_converter(df: DataFrame) -> DataFrame:
    return df.withColumn('date', f.from_unixtime('date', 'yyyy-MM-dd HH:mm:ss'))


@op
def drop_nulls(df: DataFrame) -> DataFrame:
    return df.na.drop(subset=['score', 'num_comments'])


@op
def most_comments(df: DataFrame) -> DataFrame:
    return df.orderBy(['score', 'num_comments'], ascending=[0, 1])


@graph
def make_and_filter_posts():
    most_comments(drop_nulls(epoch_converter(make_posts())))


make_and_filter_posts = make_and_filter_posts.to_job(
    resource_defs={'io_manager': local_io_manager()}
)


@repository
def reddit_dagster():
    return [make_and_filter_posts]

