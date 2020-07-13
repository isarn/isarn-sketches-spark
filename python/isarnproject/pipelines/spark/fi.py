from pyspark import since, keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaWrapper, JavaPredictionModel
from pyspark.ml.common import inherit_doc
from pyspark.sql import DataFrame

__all__ = ['TDigestFI', 'TDigestFIModel']

def toPredictionModel(value):
    if isinstance(value, JavaPredictionModel):
        return value._java_obj
    else:
        raise TypeError("object %s was not a JavaPredictionModel" % (value))

class TDigestParams(Params):
    delta = Param(Params._dummy(), "delta", "tdigest compression parameter",
                  typeConverter=TypeConverters.toFloat)
    maxDiscrete = Param(Params._dummy(), "maxDiscrete", "maximum discrete values",
                        typeConverter=TypeConverters.toInt)

    def __init__(self):
        super(TDigestParams, self).__init__()

    def setDelta(self, value):
        return self._set(delta=value)

    def getDelta(self):
        return self.getOrDefault(self.delta)

    def setMaxDiscrete(self, value):
        return self._set(maxDiscrete=value)

    def getMaxDiscrete(self):
        return self.getOrDefault(self.maxDiscrete)

class TDigestFIParams(TDigestParams, HasFeaturesCol):
    def __init__(self):
        super(TDigestFIParams, self).__init__()

class TDigestFIModelParams(HasFeaturesCol):
    targetModel = Param(Params._dummy(), "targetModel", "predictive model",
                        typeConverter=toPredictionModel)
    nameCol = Param(Params._dummy(), "nameCol", "feature name column",
                    typeConverter=TypeConverters.toString)
    importanceCol = Param(Params._dummy(), "importanceCol", "feature importance column",
                          typeConverter=TypeConverters.toString)
    deviationMeasure = Param(Params._dummy(), "deviationMeasure", "model output deviation measure",
                             typeConverter=TypeConverters.toString)
    featureNames = Param(Params._dummy(), "featureNames", "use these feature names",
                         typeConverter=TypeConverters.toListString)

    def __init__(self):
        super(TDigestFIModelParams, self).__init__()

    def setTargetModel(self, value):
        return self._set(targetModel=value)

    def getTargetModel(self):
        return self.getOrDefault(self.targetModel)

    def setNameCol(self, value):
        return self._set(nameCol=value)

    def getNameCol(self):
        return self.getOrDefault(self.nameCol)

    def setImportanceCol(self, value):
        return self._set(importanceCol=value)

    def getImportanceCol(self):
        return self.getOrDefault(self.importanceCol)

    def setDeviationMeasure(self, value):
        return self._set(deviationMeasure=value)

    def getDeviationMeasure(self):
        return self.getOrDefault(self.deviationMeasure)

    def setFeatureNames(self, value):
        return self._set(featureNames=value)

    def getFeatureNames(self):
        return self.getOrDefault(self.featureNames)

@inherit_doc
class TDigestFI(JavaEstimator, TDigestFIParams, JavaMLWritable, JavaMLReadable):
    """
    Feature Importance.
    """

    @keyword_only
    def __init__(self, delta = 0.5, maxDiscrete = 0, featuresCol = "features"):
        super(TDigestFI, self).__init__()
        self._java_obj = self._new_java_obj("org.isarnproject.pipelines.TDigestFI", self.uid)
        self._setDefault(delta = 0.5, maxDiscrete = 0, featuresCol = "features")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, delta = 0.5, maxDiscrete = 0, featuresCol = "features"):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return TDigestFIModel(java_model)

class TDigestFIModel(JavaModel, TDigestFIModelParams, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`TDigestFI`.
    """

    def __init__(self, java_model):
        # models can't accept Params from __init__
        super(TDigestFIModel, self).__init__(java_model)
        self._setDefault(deviationMeasure = "auto", featureNames = [],
                         featuresCol = "features", nameCol = "name", importanceCol = "importance")

    @keyword_only
    def setParams(self, targetModel = None, deviationMeasure = "auto", featureNames = [],
                  featuresCol = "features", nameCol = "name", importanceCol = "importance"):
        kwargs = self._input_kwargs
        # if targetModel wasn't provided then don't try to (re)set the value, it will fail
        if kwargs["targetModel"] is None:
            del kwargs["targetModel"]
        return self._set(**kwargs)
