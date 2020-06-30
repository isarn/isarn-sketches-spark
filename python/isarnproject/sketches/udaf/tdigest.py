from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.context import SparkContext

__all__ = ['tdigestIntUDAF', 'tdigestLongUDAF', 'tdigestFloatUDAF', 'tdigestDoubleUDAF', \
           'tdigestDoubleUDF', \
           'tdigestMLVecUDAF', 'tdigestMLLibVecUDAF', \
           'tdigestIntArrayUDAF', 'tdigestLongArrayUDAF', \
           'tdigestFloatArrayUDAF', 'tdigestDoubleArrayUDAF', \
           'tdigestReduceUDAF', 'tdigestArrayReduceUDAF']

def tdigestDoubleUDF(col, compression=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.java.tdigestDoubleUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestIntUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of integer data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestIntUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestLongUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of long integer data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestLongUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestFloatUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of (single precision) float data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestFloatUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of double float data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestDoubleUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestMLVecUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of ML Vector data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestMLVecUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestMLLibVecUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of MLLib Vector data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestMLLibVecUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestIntArrayUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of integer-array data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestIntArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestLongArrayUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of long-integer array data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestLongArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestFloatArrayUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of (single-precision) float array data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestFloatArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleArrayUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of double array data.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestDoubleArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestReduceUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of t-digests.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestReduceUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestArrayReduceUDAF(col, delta=0.5, maxDiscrete=0):
    """
    Return a UDAF for aggregating a column of t-digest vectors.

    :param col: name of the column to aggregate
    :param delta: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestArrayReduceUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))
