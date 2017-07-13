from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.context import SparkContext

__all__ = ['tdigestDoubleUDAF', 'tdigestDoubleUDAF2', 'tdigestDoubleUDAF3']

def tdigestDoubleUDAF(col):
    sc = SparkContext._active_spark_context
    tdudaf = sc._jvm.org.isarnproject.sketches.udaf.python.tdigestDoubleUDAF()
    tdapply = tdudaf.apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleUDAF2(col):
    sc = SparkContext._active_spark_context
    tdudaf = sc._jvm.org.isarnproject.sketches.udaf.StaticTDigestDoubleUDAF
    tdapply = tdudaf.apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleUDAF3(col):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.Static2TDigestDoubleUDAF.apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))
