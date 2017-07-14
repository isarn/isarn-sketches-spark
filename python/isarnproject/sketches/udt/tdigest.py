
import sys
import array
import struct

if sys.version >= '3':
    basestring = str
    xrange = range
    import copyreg as copy_reg
    long = int
else:
    from itertools import izip as zip
    import copy_reg

import numpy as np

from pyspark.sql.types import UserDefinedType, StructField, StructType, \
    ArrayType, DoubleType, IntegerType

__all__ = ['TDigest', 'TDigestUDT']

class TDigestUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("delta", DoubleType(), False),
            StructField("maxDiscrete", IntegerType(), False),
            StructField("nclusters", IntegerType(), False),
            StructField("clustX", ArrayType(DoubleType(), False), False),
            StructField("clustM", ArrayType(DoubleType(), False), False)])

    @classmethod
    def module(cls):
        return "isarnproject.sketches.udt.tdigest"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.isarnproject.sketches.udt.TDigestUDT"

    def simpleString(self):
        return "tdigest"

    def serialize(self, obj):
        if isinstance(obj, TDigest):
            return (obj.delta, obj.maxDiscrete, obj.nclusters, \
                    [float(v) for v in obj.clustX], \
                    [float(v) for v in obj.clustM])
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        return TDigest(datum[0], datum[1], datum[2], datum[3], datum[4])

class TDigest(object):
    __UDT__ = TDigestUDT()

    def __init__(self, delta, maxDiscrete, nclusters, clustX, clustM):
        self.delta = delta
        self.maxDiscrete = maxDiscrete
        self.nclusters = nclusters
        self.clustX = np.array(clustX, dtype=np.float64)
        self.clustM = np.array(clustM, dtype=np.float64)
        self.clustP = np.cumsum(clustM)

    def __repr__(self):
        return "TDigest(%s, %s, %s, %s, %s)" % \
            (repr(self.delta), repr(self.maxDiscrete), repr(self.nclusters), repr(self.clustX), repr(self.clustM))

    def __reduce__(self):
        return (self.__class__, (self.delta, self.maxDiscrete. self.nclusters, self.clustX, self.clustM, ))

    def __m1m2__(self, j, c1, tm1, c2, tm2):
        assert len(self.clustX) > 0, "unexpected empty TDigest"
        # s is the "open" prefix sum, for x < c1
        s = self.clustP[j] - tm1
        d1 = 0.0 if c1 == self.clustX[0] else tm1 / 2.0
        d2 = tm2 if c2 == self.clustX[-1] else tm2 / 2.0
        m1 = s + d1
        m2 = m1 + (tm1 - d1) + d2
        return (m1, m2)

    def cdf(self, xx):
        x = float(xx)
        jcov = self.coverR(x)
        cov = jcov.map(lambda j: (self.clustX[j], self.clustM[j]))
        if (not cov.l.isEmpty()) and (not cov.r.isEmpty()):
            c1, tm1 = cov.l.get()
            c2, tm2 = cov.r.get()
            m1, m2 = self.__m1m2__(jcov.l.get(), c1, tm1, c2, tm2)
            return (m1 + (x - c1) * (m2 - m1) / (c2 - c1)) / self.clustP[-1]
        if cov.r.isEmpty():
            return 1.0
        return 0.0

    def coverR(self, xx):
        n = len(self.clustX)
        if n == 0:
            return Cover(None, None)
        x = float(xx)
        j = np.searchsorted(self.clustX, x, side='right')
        if j == n:
            return Cover(n - 1, None)
        if x < self.clustX[0]:
            return Cover(None, 0)
        return Cover(j - 1, j)

class Cover(object):
    def __repr__(self):
        return "Cover(%s, %s)" % (repr(self.l), repr(self.r))

    def __init__(self, l, r):
        self.l = Option(l)
        self.r = Option(r)

    def map(self, f):
        assert hasattr(f, '__call__'), "f must be callable"
        return Cover(self.l.map(f).value, self.r.map(f).value)

class Option(object):
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
