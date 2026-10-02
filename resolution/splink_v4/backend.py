"""Small, version-pinned Spark Connect adapter; never patches global Splink state.

Upstream SparkAPI eagerly calls Databricks' classic JVM setup even when JAR
registration is disabled. This initializer uses its SQL backend without that
setup. Matching, EM, term frequencies and scoring remain upstream Splink.
Only native Spark SQL comparisons are permitted by our model settings.
"""

from importlib.metadata import version

from splink.internals.database_api import DatabaseAPI
from splink.internals.spark.database_api import SparkAPI

from . import SPLINK_VERSION


class ServerlessSparkAPI(SparkAPI):
    def __init__(self, spark_session, catalog, database):
        if version("splink") != SPLINK_VERSION:
            raise RuntimeError(f"Adapter requires splink=={SPLINK_VERSION}")
        DatabaseAPI.__init__(self)
        self.spark = spark_session
        self.break_lineage_method = "delta_lake_table"
        self.repartition_after_blocking = False
        self.num_partitions_on_repartition = 16
        self.in_databricks = True
        self._set_splink_datastore(catalog, database)

    def _table_registration(self, input, table_name):
        # Connect DataFrames aren't subclasses of classic pyspark.sql.DataFrame.
        if hasattr(input, "createOrReplaceTempView"):
            input.createOrReplaceTempView(table_name)
        else:
            super()._table_registration(input, table_name)

    def _repartition_if_needed(self, spark_df, templated_name):
        return spark_df


def predict_candidate_pairs(linker, candidates):
    """Score external canonical pairs using Splink's *same* inference SQL.

The only substituted step is blocking. `candidates` has left_record_key and
right_record_key. These private APIs are covered by a stock-predict parity test
on the deployed runtime and are pinned to Splink 4.0.17.
"""
    from pyspark.sql import functions as F
    from splink.internals.comparison_vector_values import (
        compute_comparison_vector_values_from_id_pairs_sqls,
    )
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.predict import predict_from_comparison_vectors_sqls_using_settings
    from splink.internals.vertically_concatenate import compute_df_concat_with_tf

    nodes = compute_df_concat_with_tf(linker, CTEPipeline())
    pairs = candidates.select(
        F.col("left_record_key").alias("join_key_l"),
        F.col("right_record_key").alias("join_key_r"),
        F.lit("0").alias("match_key"),
    )
    blocked = linker.table_management.register_table(
        pairs, "__splink__external_pairs_" + linker._cache_uid, overwrite=True
    )
    blocked.templated_name = "__splink__blocked_id_pairs"
    pipeline = CTEPipeline([nodes, blocked])
    settings = linker._settings_obj
    pipeline.enqueue_list_of_sqls(compute_comparison_vector_values_from_id_pairs_sqls(
        settings._columns_to_select_for_blocking,
        settings._columns_to_select_for_comparison_vector_values,
        input_tablename_l="__splink__df_concat_with_tf",
        input_tablename_r="__splink__df_concat_with_tf",
        source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
        unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        link_type=settings._link_type,
        sql_dialect_str=linker._sql_dialect_str,
    ))
    pipeline.enqueue_list_of_sqls(predict_from_comparison_vectors_sqls_using_settings(
        settings, None, None, sql_infinity_expression=linker._infinity_expression,
    ))
    return linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
