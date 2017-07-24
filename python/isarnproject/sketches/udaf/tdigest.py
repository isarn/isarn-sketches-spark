from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.context import SparkContext

__all__ = ['tdigestIntUDAF', 'tdigestLongUDAF', 'tdigestFloatUDAF', 'tdigestDoubleUDAF', \
           'tdigestMLVecUDAF', 'tdigestMLLibVecUDAF', \
           'tdigestIntArrayUDAF', 'tdigestLongArrayUDAF', \
           'tdigestFloatArrayUDAF', 'tdigestDoubleArrayUDAF' ]

def tdigestIntUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestIntUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestLongUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestLongUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestFloatUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestFloatUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestDoubleUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestMLVecUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestMLVecUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestMLLibVecUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestMLLibVecUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestIntArrayUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestIntArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestLongArrayUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestLongArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestFloatArrayUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestFloatArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

def tdigestDoubleArrayUDAF(col, delta=0.5, maxDiscrete=0):
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.udaf.pythonBindings.tdigestDoubleArrayUDAF( \
        delta, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))
