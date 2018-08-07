
import sys
import random

import numpy as np

from pyspark.sql.types import UserDefinedType, StructField, StructType, \
    ArrayType, DoubleType, IntegerType

__all__ = ['TDigest']

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

class TDigestArrayUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("delta", DoubleType(), False),
            StructField("maxDiscrete", IntegerType(), False),
            StructField("clusterS", ArrayType(IntegerType(), False), False),
            StructField("clusterX", ArrayType(DoubleType(), False), False),
            StructField("clusterM", ArrayType(DoubleType(), False), False)])

    @classmethod
    def module(cls):
        return "isarnproject.sketches.udt.tdigest"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.isarnproject.sketches.udt.TDigestArrayUDT"

    def simpleString(self):
        return "tdigestarray"

    def serialize(self, obj):
        if isinstance(obj, TDigestList):
            clustS = []
            clustX = []
            clustM = []
            for td in obj:
                clustS.append(td.nclusters)
                clustX += [float(v) for v in td.clustX]
                clustM += [float(v) for v in td.clustM]
            delta = obj[0].delta if len(obj) > 0 else 0.5
            maxDiscrete = obj[0].maxDiscrete if len(obj) > 0 else 0
            return (delta, maxDiscrete, clustS, clustX, clustM)
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        delta = datum[0]
        maxDiscrete = datum[1]
        clusterS = datum[2]
        clusterX = datum[3]
        clusterM = datum[4]
        tdlist = TDigestList()
        b = 0
        for s in clusterS:
            clustX = clusterX[b:b+s]
            clustM = clusterM[b:b+s]
            tdlist.append(TDigest(delta, maxDiscrete, s, clustX, clustM))
            b += s
        return tdlist

class TDigestList(list):
    """
    A subclass of a list of TDigest objects, deserialized from a Dataset.
    This subclass has a __UDT__ element
    """

    __UDT__ = TDigestArrayUDT()

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

    def __init__(self, delta, maxDiscrete, nclusters, clustX, clustM):
        self.delta = float(delta)
        self.maxDiscrete = int(maxDiscrete)
        self.nclusters = int(nclusters)
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
