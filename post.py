from pyspark.sql import functions as f
from pyspark.sql import types as T


class Post:

    def __init__(self, spark, filepath):
        self.spark = spark
        self.filepath = filepath
        self.columns = ['title', 'score', 'author', 'total_awards_received', 'date', 'full_link', 'num_comments']

    def create_df_from_csv(self):
        """
        Create a SparkDataframe from a csv file.
        :return: SparkDataframe
        """
        df = self.spark.read.csv(self.filepath, header=True, inferSchema=True)
        return df

    def build_df(self, df):
        """
        Return a SparkDataFrame with the necessary columns and available posts.
        :param df: SparkDataframe
        :return: SparkDataframe
        """
        df_rename = df.withColumnRenamed('created_utc', 'date')
        # Filter to show only available posts (removed_by == 'null')
        df_filter = df_rename.filter(df_rename.removed_by.isNull())
        return df_filter.select(*self.columns)

    @staticmethod
    def epoch_converter(df):
        """
        Convert epoch to datetime
        :param df: SparkDataframe
        :return: SparkDataframe
        """
        return df.withColumn('date', f.from_unixtime('date', 'yyyy-MM-dd HH:mm:ss'))

    @staticmethod
    def most_comments(df):
        """
        Sort the posts by the number of comments
        :param df: SparkDataframe
        :return: SparkDataframe
        """
        return df.orderBy(['score', 'num_comments'], ascending=[0, 1])

    @staticmethod
    def cast_columns(df):
        """
        Cast columns to the correct type
        :param df: SparkDataframe
        :return: SparkDataframe
        """
        return df.withColumn('score', df.score.cast(T.IntegerType()))\
            .withColumn('num_comments', df.num_comments.cast(T.IntegerType()))\
            .withColumn('total_awards_received', df.total_awards_received.cast(T.IntegerType()))\
            .withColumn('date', df.date.cast(T.TimestampType()))

    @staticmethod
    def drop_nulls(df):
        """
        Drop rows with null values
        :param df:
        :return:
        """
        return df.na.drop(subset=['score', 'num_comments'])



