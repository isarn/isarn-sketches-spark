from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.context import SparkContext

__all__ = ['tdigestIntUDF', 'tdigestLongUDF', 'tdigestFloatUDF', 'tdigestDoubleUDF', \
           'tdigestMLVecUDF', 'tdigestMLLibVecUDF', \
           'tdigestIntArrayUDF', 'tdigestLongArrayUDF', \
           'tdigestFloatArrayUDF', 'tdigestDoubleArrayUDF', \
           'tdigestReduceUDF', 'tdigestArrayReduceUDF']

def tdigestIntUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of integer data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestIntUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestLongUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of long integer data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestLongUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestFloatUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of (single precision) float data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestFloatUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of double float data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestDoubleUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestMLVecUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of ML Vector data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestMLVecUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestMLLibVecUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of MLLib Vector data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestMLLibVecUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestIntArrayUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of integer-array data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestIntArrayUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestLongArrayUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of long-integer array data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestLongArrayUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestFloatArrayUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of (single-precision) float array data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestFloatArrayUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleArrayUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of double array data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestDoubleArrayUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestReduceUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of t-digests.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestReduceUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestArrayReduceUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of t-digest vectors.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.functions.tdigestArrayReduceUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))
