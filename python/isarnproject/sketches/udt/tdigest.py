
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
        return TDigest(datum[0], datum[1], datum[2], \
                       np.array(datum[3], dtype=np.float64), \
                       np.array(datum[4], dtype=np.float64))

class TDigest(object):
    __UDT__ = TDigestUDT()

    def __init__(self, delta, maxDiscrete, nclusters, clustX, clustM):
        self.delta = delta
        self.maxDiscrete = maxDiscrete
        self.nclusters = nclusters
        self.clustX = clustX
        self.clustM = clustM

    def __repr__(self):
        return "TDigest(%r, %r, %r, %r, %r)" % (self.delta, self.maxDiscrete, self.nclusters, self.clustX, self.clustM)

    def __reduce__(self):
        return (self.__class__, (self.delta, self.maxDiscrete. self.nclusters, self.clustX, self.clustM, ))
