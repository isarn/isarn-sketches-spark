import sys
import random

import numpy as np

from pyspark.sql.types import UserDefinedType, StructField, StructType, \
    ArrayType, DoubleType, IntegerType
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.context import SparkContext

__all__ = ['tdigestIntUDF', 'tdigestLongUDF', 'tdigestFloatUDF', 'tdigestDoubleUDF', \
           'tdigestMLVecUDF', 'tdigestMLLibVecUDF', \
           'tdigestIntArrayUDF', 'tdigestLongArrayUDF', 'tdigestFloatArrayUDF', 'tdigestDoubleArrayUDF', \
           'tdigestReduceUDF', 'tdigestArrayReduceUDF', \
           'TDigest']

def tdigestIntUDF(col, compression=0.5, maxDiscrete=0):
    """
    Return a UDF for aggregating a column of integer data.

    :param col: name of the column to aggregate
    :param compression: T-Digest compression parameter (default 0.5)
    :param maxDiscrete: maximum unique discrete values to store before reverting to
        continuous (default 0)
    """
    sc = SparkContext._active_spark_context
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestIntUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestLongUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestFloatUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestDoubleUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestMLVecUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestMLLibVecUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestIntArrayUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestLongArrayUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestFloatArrayUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestDoubleArrayUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestReduceUDF( \
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
    tdapply = sc._jvm.org.isarnproject.sketches.spark.tdigest.functions.tdigestArrayReduceUDF( \
        compression, maxDiscrete).apply
    return Column(tdapply(_to_seq(sc, [col], _to_java_column)))

class TDigestUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("compression", DoubleType(), False),
            StructField("maxDiscrete", IntegerType(), False),
            StructField("cent", ArrayType(DoubleType(), False), False),
            StructField("mass", ArrayType(DoubleType(), False), False)])

    @classmethod
    def module(cls):
        return "isarnproject.sketches.udt.tdigest"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.isarnproject.sketches.udtdev.TDigestUDT"

    def simpleString(self):
        return "tdigest"

    def serialize(self, obj):
        if isinstance(obj, TDigest):
            return (obj.delta, obj.maxDiscrete, \
                    [float(v) for v in obj.clustX], \
                    [float(v) for v in obj.clustM])
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        return TDigest(datum[0], datum[1], datum[2], datum[3])

class TDigest(object):
    """
    A T-Digest sketch of a cumulative numeric distribution.
    This is a "read-only" python mirror of org.isarnproject.sketches.TDigest which supports
    all cdf and sampling methods, but does not currently support update with new data. It is
    assumed to have been produced with a t-digest UDAF, also exposed in this package.
    """

    # Because this is a value and not a function, TDigestUDT has to be defined above,
    # and in the same file.
    __UDT__ = TDigestUDT()

    def __init__(self, delta, maxDiscrete, clustX, clustM):
        self.delta = float(delta)
        self.maxDiscrete = int(maxDiscrete)
        self.nclusters = len(clustX)
        self.clustX = np.array(clustX, dtype=np.float64)
        self.clustM = np.array(clustM, dtype=np.float64)
        self.clustP = np.cumsum(clustM)
        assert len(self.clustX) == self.nclusters, "nclusters does not match cluster mass array"
        assert len(self.clustX) == len(self.clustM), "cluster mass array does not match cluster center array"

    def __repr__(self):
        return "TDigest(%s, %s, %s, %s, %s)" % \
            (repr(self.delta), repr(self.maxDiscrete), repr(self.nclusters), repr(self.clustX), repr(self.clustM))

    def mass(self):
        """
        Total mass accumulated by this TDigest
        """
        if len(self.clustP) == 0:
            return 0.0
        return self.clustP[-1]

    def isEmpty(self):
        """
        Returns True if this TDigest is empty, False otherwise
        """
        return len(self.clustX) == 0

    def __reduce__(self):
        return (self.__class__, (self.delta, self.maxDiscrete, self.nclusters, self.clustX, self.clustM, ))

    # The "right cover" of a value x, w.r.t. the clusters in this TDigest
    def __covR__(self, x):
        n = len(self.clustX)
        if n == 0:
            return Cover(None, None)
        j = np.searchsorted(self.clustX, x, side='right')
        if j == n:
            return Cover(n - 1, None)
        if x < self.clustX[0]:
            return Cover(None, 0)
        return Cover(j - 1, j)

    # The "mass cover" of a mass m, w.r.t. the clusters in this TDigest
    def __covM__(self, m):
        n = len(self.clustP)
        if n == 0:
            return Cover(None, None)
        j = np.searchsorted(self.clustP, m, side='right')
        if j == n:
            return Cover(n - 1, None)
        if m < self.clustP[0]:
            return Cover(None, 0)
        return Cover(j - 1, j)

    # Get a "corrected mass" for two clusters.
    # Returns "centered" mass for interior clusters, and uncentered for edge clusters.
    def __m1m2__(self, j, c1, tm1, c2, tm2):
        assert len(self.clustX) > 0, "unexpected empty TDigest"
        # s is the "open" prefix sum, for x < c1
        s = self.clustP[j] - tm1
        d1 = 0.0 if c1 == self.clustX[0] else tm1 / 2.0
        d2 = tm2 if c2 == self.clustX[-1] else tm2 / 2.0
        m1 = s + d1
        m2 = m1 + (tm1 - d1) + d2
        return (m1, m2)

    # Get the inverse CDF, using the "corrected mass"
    def __cdfI__(self, j, m, c1, tm1, c2, tm2):
        m1, m2 = self.__m1m2__(j, c1, tm1, c2, tm2)
        x = c1 + (m - m1) * (c2 - c1) / (m2 - m1)
        return min(c2, max(c1, x))

    def cdf(self, xx):
        """
        Return CDF(x) of a numeric value x, with respect to this TDigest CDF sketch.
        """
        x = float(xx)
        jcov = self.__covR__(x)
        cov = jcov.map(lambda j: (self.clustX[j], self.clustM[j]))
        if (not cov.l.isEmpty()) and (not cov.r.isEmpty()):
            c1, tm1 = cov.l.get()
            c2, tm2 = cov.r.get()
            m1, m2 = self.__m1m2__(jcov.l.get(), c1, tm1, c2, tm2)
            m = m1 + (x - c1) * (m2 - m1) / (c2 - c1)
            return min(m2, max(m1, m)) / self.mass()
        if cov.r.isEmpty():
            return 1.0
        return 0.0

    def cdfInverse(self, qq):
        """
        Given a value q on [0,1], return the value x such that CDF(x) = q.
        Returns NaN for any q > 1 or < 0, or if this TDigest is empty.
        """
        q = float(qq)
        if (q < 0.0) or (q > 1.0):
            return float('nan')
        if self.isEmpty():
            return float('nan')
        jcov = self.__covM__(q * self.mass())
        cov = jcov.map(lambda j: (self.clustX[j], self.clustM[j]))
        m = q * self.mass()
        if (not cov.l.isEmpty()) and (not cov.r.isEmpty()):
            c1, tm1 = cov.l.get()
            c2, tm2 = cov.r.get()
            return self.__cdfI__(jcov.l.get(), m, c1, tm1, c2, tm2)
        if not cov.r.isEmpty():
            c = cov.r.get()[0]
            jcovR = self.__covR__(c)
            covR = jcovR.map(lambda j: (self.clustX[j], self.clustM[j]))
            if (not covR.l.isEmpty()) and (not covR.r.isEmpty()):
                c1, tm1 = covR.l.get()
                c2, tm2 = covR.r.get()
                return self.__cdfI__(jcovR.l.get(), m, c1, tm1, c2, tm2)
            return float('nan')
        if not cov.l.isEmpty():
            c = cov.l.get()[0]
            return c
        return float('nan')

    def cdfDiscrete(self, xx):
        """
        return CDF(x) for a numeric value x, assuming the sketch is representing a
        discrete distribution.
        """
        if self.isEmpty():
            return 0.0
        j = np.searchsorted(self.clustX, float(xx), side='right')
        if (j == 0):
            return 0.0
        return self.clustP[j - 1] / self.mass()

    def cdfDiscreteInverse(self, qq):
        """
        Given a value q on [0,1], return the value x such that CDF(x) = q, assuming
        the sketch is represenging a discrete distribution.
        Returns NaN for any q > 1 or < 0, or if this TDigest is empty.
        """        
        q = float(qq)
        if (q < 0.0) or (q > 1.0):
            return float('nan')
        if self.isEmpty():
            return float('nan')
        j = np.searchsorted(self.clustP, q * self.mass(), side='left')
        return self.clustX[j]

    def samplePDF(self):
        """
        Return a random sampling from the sketched distribution, using inverse
        transform sampling, assuming a continuous distribution.
        """
        return self.cdfInverse(random.random())

    def samplePMF(self):
        """
        Return a random sampling from the sketched distribution, using inverse
        transform sampling, assuming a discrete distribution.
        """
        return self.cdfDiscreteInverse(random.random())

    def sample(self):
        """
        Return a random sampling from the sketched distribution, using inverse
        transform sampling, assuming a discrete distribution if the number of
        TDigest clusters is <= maxDiscrete, and a continuous distribution otherwise.
        """
        if self.maxDiscrete <= self.nclusters:
            return self.cdfDiscreteInverse(random.random())
        return self.cdfInverse(random.random())

class Cover(object):
    """
    Analog of org.isarnproject.collections.mixmaps.nearest.Cover[Any].
    Not intended for public consumption.
    """
    def __repr__(self):
        return "Cover(%s, %s)" % (repr(self.l), repr(self.r))

    def __init__(self, l, r):
        self.l = Option(l)
        self.r = Option(r)

    def map(self, f):
        assert hasattr(f, '__call__'), "f must be callable"
        return Cover(self.l.map(f).value, self.r.map(f).value)

class Option(object):
    """
    Analog of Scala Option[Any]. I only implemented the methods I need.
    Not intented for public consumption.
    """
    def __repr__(self):
        return "Option(%s)" % (repr(self.value))

    def __init__(self, v):
        self.value = v

    def get(self):
        assert self.value is not None, "Opt value was None"
        return self.value

    def map(self, f):
        assert hasattr(f, '__call__'), "f must be callable"
        if self.value is None:
            return Option(None)
        return Option(f(self.value))

    def isEmpty(self):
        return self.value is None
