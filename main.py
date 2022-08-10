from pyspark.sql import SparkSession

from post import Post


def spark_session():
    """
    Function that initialize spark session
    """
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def main():
    posts = Post(spark_session(), "data/r_dataisbeautiful_posts.csv")
    df_post = posts.create_df_from_csv()
    df_filter = posts.build_df(df_post)
    df_epoch = posts.epoch_converter(df_filter)
    df_cast = posts.cast_columns(df_epoch)
    df_drop_null = posts.drop_nulls(df_cast)
    df_most_comments = posts.most_comments(df_drop_null)
    df_most_comments.show()


if __name__ == '__main__':
    main()
