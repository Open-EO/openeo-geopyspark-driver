
import collections
import datetime
from enum import Enum

from py4j.java_gateway import JavaObject

from pyspark import keyword_only, SparkContext


from pyspark.ml.classification import _JavaProbabilisticClassificationModel
from pyspark.ml.regression import _JavaRegressionModel


import pyspark.ml.common
from pyspark.ml.common import inherit_doc
from pyspark.ml.param import Param, Params
from pyspark.ml.util import JavaMLReader, JavaMLWriter, JavaMLWritable, MLReadable


import pyspark.ml.wrapper
from pyspark.ml.wrapper import JavaParams, JavaEstimator, JavaWrapper
from pyspark.sql import DataFrame, SparkSession


"""
    original JavaParams._from_java has to be replaced because of hardcoded class names transformation
"""

@staticmethod
def _from_java_patched_for_catboost(java_stage):
    """
    Given a Java object, create and return a Python wrapper of it.
    Used for ML persistence.

    Meta-algorithms such as Pipeline should override this method as a classmethod.
    """
    def __get_class(clazz):
        """
        Loads Python class from its name.
        """
        parts = clazz.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    stage_name = (
        java_stage.getClass().getName()
            .replace("org.apache.spark", "pyspark")
            .replace("ai.catboost.spark", "catboost_spark")
    )
    # Generate a default new instance from the stage_name class.
    py_type = __get_class(stage_name)
    if issubclass(py_type, JavaParams):
        # Load information from java_stage to the instance.
        py_stage = py_type()
        py_stage._java_obj = java_stage
        py_stage._resetUid(java_stage.uid())
        py_stage._transfer_params_from_java()
    elif hasattr(py_type, "_from_java"):
        py_stage = py_type._from_java(java_stage)
    else:
        raise NotImplementedError("This Java stage cannot be loaded into Python currently: %r"
                                  % stage_name)
    return py_stage

JavaParams._from_java = _from_java_patched_for_catboost


"""
    Adapt _py2java and _java2py for additional types present in CatBoost Params
"""

_standard_py2java = pyspark.ml.common._py2java
_standard_java2py = pyspark.ml.common._java2py

def _py2java(sc, obj):
    """ Convert Python object into Java """
    if isinstance(obj, SparkSession):
        return obj._jsparkSession
    if isinstance(obj, Enum):
        return getattr(
            getattr(
                sc._jvm.ru.yandex.catboost.spark.catboost4j_spark.core.src.native_impl, 
                obj.__class__.__name__
            ),
            'swigToEnum'
        )(obj.value)
    if isinstance(obj, datetime.timedelta):
        return sc._jvm.java.time.Duration.ofMillis(obj // datetime.timedelta(milliseconds=1))
    if isinstance(obj, JavaParams):
        return obj._to_java()
    if isinstance(obj, collections.OrderedDict):
        return sc._jvm.java.util.LinkedHashMap(obj)
    return _standard_py2java(sc, obj)

def _java2py(sc, r, encoding="bytes"):
    if isinstance(r, JavaObject):
        enumValues = r.getClass().getEnumConstants()
        if (enumValues is not None) and (len(enumValues) > 0):
            return globals()[r.getClass().getSimpleName()](r.swigValue())
        
        clsName = r.getClass().getName()
        if clsName == 'java.time.Duration':
            return datetime.timedelta(milliseconds=r.toMillis())
        if clsName == 'ai.catboost.spark.Pool':
            return Pool(r)
        if clsName == 'java.util.LinkedHashMap':
            return collections.OrderedDict(r)
    return _standard_java2py(sc, r, encoding)

pyspark.ml.common._py2java = _py2java
pyspark.ml.common._java2py = _java2py

pyspark.ml.wrapper._py2java = _py2java
pyspark.ml.wrapper._java2py = _java2py


@inherit_doc
class CatBoostMLReader(JavaMLReader):
    """
    (Private) Specialization of :py:class:`JavaMLReader` for CatBoost types
    """

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        Returns the full class name of the Java ML instance.
        """
        java_package = clazz.__module__.replace("catboost_spark.core", "ai.catboost.spark")
        print("CatBoostMLReader._java_loader_class. ", java_package + "." + clazz.__name__)
        return java_package + "." + clazz.__name__



class PoolLoadParams(JavaParams):
    """
    Parameters
    ----------
    delimiter : str, default: "\t"
        The delimiter character used to separate the data in the dataset description input file.
    hasHeader : bool
        Read the column names from the first line of the dataset description file if this parameter is set.
    """

    @keyword_only
    def __init__(self, delimiter="\t", hasHeader=None):
        super(PoolLoadParams, self).__init__()
        self._java_obj = self._new_java_obj("ai.catboost.spark.params.PoolLoadParams")
        self.delimiter = Param(self, "delimiter", "The delimiter character used to separate the data in the dataset description input file.")
        self._setDefault(delimiter="\t")
        self.hasHeader = Param(self, "hasHeader", "Read the column names from the first line of the dataset description file if this parameter is set.")

        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)


    @keyword_only
    def setParams(self, delimiter="\t", hasHeader=None):
        """
        Set the (keyword only) parameters

        Parameters
        ----------
        delimiter : str, default: "\t"
            The delimiter character used to separate the data in the dataset description input file.
        hasHeader : bool
            Read the column names from the first line of the dataset description file if this parameter is set.
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)


    def getDelimiter(self):
        """
        Returns
        -------
        str
            The delimiter character used to separate the data in the dataset description input file.
        """
        return self.getOrDefault(self.delimiter)

    def setDelimiter(self, value):
        """
        Parameters
        ----------
        value : str
            The delimiter character used to separate the data in the dataset description input file.
        """
        self._set(delimiter=value)
        return self



    def getHasHeader(self):
        """
        Returns
        -------
        bool
            Read the column names from the first line of the dataset description file if this parameter is set.
        """
        return self.getOrDefault(self.hasHeader)

    def setHasHeader(self, value):
        """
        Parameters
        ----------
        value : bool
            Read the column names from the first line of the dataset description file if this parameter is set.
        """
        self._set(hasHeader=value)
        return self





class QuantizationParams(JavaParams):
    """
    Parameters
    ----------
    borderCount : int
        The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
    featureBorderType : EBorderSelectionType
        The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
    ignoredFeaturesIndices : list
        Feature indices to exclude from the training
    ignoredFeaturesNames : list
        Feature names to exclude from the training
    inputBorders : str
        Load Custom quantization borders and missing value modes from a file (do not generate them)
    nanMode : ENanMode
        The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
    perFloatFeatureQuantizaton : list
        The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
    threadCount : int
        Number of CPU threads in parallel operations on client
    """

    @keyword_only
    def __init__(self, borderCount=None, featureBorderType=None, ignoredFeaturesIndices=None, ignoredFeaturesNames=None, inputBorders=None, nanMode=None, perFloatFeatureQuantizaton=None, threadCount=None):
        super(QuantizationParams, self).__init__()
        self._java_obj = self._new_java_obj("ai.catboost.spark.params.QuantizationParams")
        self.borderCount = Param(self, "borderCount", "The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.")
        self.featureBorderType = Param(self, "featureBorderType", "The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'")
        self.ignoredFeaturesIndices = Param(self, "ignoredFeaturesIndices", "Feature indices to exclude from the training")
        self.ignoredFeaturesNames = Param(self, "ignoredFeaturesNames", "Feature names to exclude from the training")
        self.inputBorders = Param(self, "inputBorders", "Load Custom quantization borders and missing value modes from a file (do not generate them)")
        self.nanMode = Param(self, "nanMode", "The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'")
        self.perFloatFeatureQuantizaton = Param(self, "perFloatFeatureQuantizaton", "The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]")
        self.threadCount = Param(self, "threadCount", "Number of CPU threads in parallel operations on client")

        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)


    @keyword_only
    def setParams(self, borderCount=None, featureBorderType=None, ignoredFeaturesIndices=None, ignoredFeaturesNames=None, inputBorders=None, nanMode=None, perFloatFeatureQuantizaton=None, threadCount=None):
        """
        Set the (keyword only) parameters

        Parameters
        ----------
        borderCount : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        featureBorderType : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        ignoredFeaturesIndices : list
            Feature indices to exclude from the training
        ignoredFeaturesNames : list
            Feature names to exclude from the training
        inputBorders : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        nanMode : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        perFloatFeatureQuantizaton : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        threadCount : int
            Number of CPU threads in parallel operations on client
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)


    def getBorderCount(self):
        """
        Returns
        -------
        int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        """
        return self.getOrDefault(self.borderCount)

    def setBorderCount(self, value):
        """
        Parameters
        ----------
        value : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        """
        self._set(borderCount=value)
        return self



    def getFeatureBorderType(self):
        """
        Returns
        -------
        EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        """
        return self.getOrDefault(self.featureBorderType)

    def setFeatureBorderType(self, value):
        """
        Parameters
        ----------
        value : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        """
        self._set(featureBorderType=value)
        return self



    def getIgnoredFeaturesIndices(self):
        """
        Returns
        -------
        list
            Feature indices to exclude from the training
        """
        return self.getOrDefault(self.ignoredFeaturesIndices)

    def setIgnoredFeaturesIndices(self, value):
        """
        Parameters
        ----------
        value : list
            Feature indices to exclude from the training
        """
        self._set(ignoredFeaturesIndices=value)
        return self



    def getIgnoredFeaturesNames(self):
        """
        Returns
        -------
        list
            Feature names to exclude from the training
        """
        return self.getOrDefault(self.ignoredFeaturesNames)

    def setIgnoredFeaturesNames(self, value):
        """
        Parameters
        ----------
        value : list
            Feature names to exclude from the training
        """
        self._set(ignoredFeaturesNames=value)
        return self



    def getInputBorders(self):
        """
        Returns
        -------
        str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        """
        return self.getOrDefault(self.inputBorders)

    def setInputBorders(self, value):
        """
        Parameters
        ----------
        value : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        """
        self._set(inputBorders=value)
        return self



    def getNanMode(self):
        """
        Returns
        -------
        ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        """
        return self.getOrDefault(self.nanMode)

    def setNanMode(self, value):
        """
        Parameters
        ----------
        value : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        """
        self._set(nanMode=value)
        return self



    def getPerFloatFeatureQuantizaton(self):
        """
        Returns
        -------
        list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        """
        return self.getOrDefault(self.perFloatFeatureQuantizaton)

    def setPerFloatFeatureQuantizaton(self, value):
        """
        Parameters
        ----------
        value : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        """
        self._set(perFloatFeatureQuantizaton=value)
        return self



    def getThreadCount(self):
        """
        Returns
        -------
        int
            Number of CPU threads in parallel operations on client
        """
        return self.getOrDefault(self.threadCount)

    def setThreadCount(self, value):
        """
        Parameters
        ----------
        value : int
            Number of CPU threads in parallel operations on client
        """
        self._set(threadCount=value)
        return self





class Pool(JavaParams):
    """
    CatBoost's abstraction of a dataset.
    Features data can be stored in raw (features column has pyspark.ml.linalg.Vector type)
    or quantized (float feature values are quantized into integer bin values, features column has
    Array[Byte] type) form.

    Raw Pool can be transformed to quantized form using `quantize` method.
    This is useful if this dataset is used for training multiple times and quantization parameters do not
    change. Pre-quantized Pool allows to cache quantized features data and so do not re-run
    feature quantization step at the start of an each training.
    """
    def __init__(self, data_frame_or_java_object, pairs_data_frame=None):
        """
        Construct Pool from DataFrame, optionally specifying pairs data in an additional DataFrame.
        """
        if isinstance(data_frame_or_java_object, JavaObject):
            java_obj = data_frame_or_java_object
        else:
            java_obj = JavaWrapper._new_java_obj("ai.catboost.spark.Pool", data_frame_or_java_object, pairs_data_frame)

        super(Pool, self).__init__(java_obj)
        self.baselineCol = Param(self, "baselineCol", "baseline column name")
        self.featuresCol = Param(self, "featuresCol", "features column name")
        self._setDefault(featuresCol="features")
        self.groupIdCol = Param(self, "groupIdCol", "groupId column name")
        self.groupWeightCol = Param(self, "groupWeightCol", "groupWeight column name")
        self.labelCol = Param(self, "labelCol", "label column name")
        self._setDefault(labelCol="label")
        self.sampleIdCol = Param(self, "sampleIdCol", "sampleId column name")
        self.subgroupIdCol = Param(self, "subgroupIdCol", "subgroupId column name")
        self.timestampCol = Param(self, "timestampCol", "timestamp column name")
        self.weightCol = Param(self, "weightCol", "weight column name. If this is not set or empty, we treat all instance weights as 1.0")
      

    @keyword_only
    def setParams(self, baselineCol=None, featuresCol="features", groupIdCol=None, groupWeightCol=None, labelCol="label", sampleIdCol=None, subgroupIdCol=None, timestampCol=None, weightCol=None):
        """
        Set the (keyword only) parameters

        Parameters
        ----------
        baselineCol : str
            baseline column name
        featuresCol : str, default: "features"
            features column name
        groupIdCol : str
            groupId column name
        groupWeightCol : str
            groupWeight column name
        labelCol : str, default: "label"
            label column name
        sampleIdCol : str
            sampleId column name
        subgroupIdCol : str
            subgroupId column name
        timestampCol : str
            timestamp column name
        weightCol : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)


    def getBaselineCol(self):
        """
        Returns
        -------
        str
            baseline column name
        """
        return self.getOrDefault(self.baselineCol)

    def setBaselineCol(self, value):
        """
        Parameters
        ----------
        value : str
            baseline column name
        """
        self._set(baselineCol=value)
        return self



    def getFeaturesCol(self):
        """
        Returns
        -------
        str
            features column name
        """
        return self.getOrDefault(self.featuresCol)

    def setFeaturesCol(self, value):
        """
        Parameters
        ----------
        value : str
            features column name
        """
        self._set(featuresCol=value)
        return self



    def getGroupIdCol(self):
        """
        Returns
        -------
        str
            groupId column name
        """
        return self.getOrDefault(self.groupIdCol)

    def setGroupIdCol(self, value):
        """
        Parameters
        ----------
        value : str
            groupId column name
        """
        self._set(groupIdCol=value)
        return self



    def getGroupWeightCol(self):
        """
        Returns
        -------
        str
            groupWeight column name
        """
        return self.getOrDefault(self.groupWeightCol)

    def setGroupWeightCol(self, value):
        """
        Parameters
        ----------
        value : str
            groupWeight column name
        """
        self._set(groupWeightCol=value)
        return self



    def getLabelCol(self):
        """
        Returns
        -------
        str
            label column name
        """
        return self.getOrDefault(self.labelCol)

    def setLabelCol(self, value):
        """
        Parameters
        ----------
        value : str
            label column name
        """
        self._set(labelCol=value)
        return self



    def getSampleIdCol(self):
        """
        Returns
        -------
        str
            sampleId column name
        """
        return self.getOrDefault(self.sampleIdCol)

    def setSampleIdCol(self, value):
        """
        Parameters
        ----------
        value : str
            sampleId column name
        """
        self._set(sampleIdCol=value)
        return self



    def getSubgroupIdCol(self):
        """
        Returns
        -------
        str
            subgroupId column name
        """
        return self.getOrDefault(self.subgroupIdCol)

    def setSubgroupIdCol(self, value):
        """
        Parameters
        ----------
        value : str
            subgroupId column name
        """
        self._set(subgroupIdCol=value)
        return self



    def getTimestampCol(self):
        """
        Returns
        -------
        str
            timestamp column name
        """
        return self.getOrDefault(self.timestampCol)

    def setTimestampCol(self, value):
        """
        Parameters
        ----------
        value : str
            timestamp column name
        """
        self._set(timestampCol=value)
        return self



    def getWeightCol(self):
        """
        Returns
        -------
        str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        """
        return self.getOrDefault(self.weightCol)

    def setWeightCol(self, value):
        """
        Parameters
        ----------
        value : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        """
        self._set(weightCol=value)
        return self




    def _call_java(self, name, *args):
        self._transfer_params_to_java()
        return JavaWrapper._call_java(self, name, *args)


    def isQuantized(self):
        """
        Returns whether the main `data` has already been quantized.
        """
        return self._call_java("isQuantized")


    def getFeatureCount(self):
        """
        Returns the number of features.
        """
        return self._call_java("getFeatureCount")


    def getFeatureNames(self):
        """
        Returns the list of feature names.
        """
        return self._call_java("getFeatureNames")


    def count(self):
        """
        Returns the number of rows in the main `data` DataFrame.
        """
        return self._call_java("count")


    def pairsCount(self):
        """
        Returns the number of rows in the `pairsData` DataFrame.
        """
        return self._call_java("pairsCount")


    def getBaselineCount(self):
        """
        Returns the dimension of the baseline data (0 if not specified).
        """
        return self._call_java("getBaselineCount")


    @property
    def data(self):
        """
        DataFrame with the main data (features, label, (optionally) weight etc.)
        """
        return self._call_java("data")

    @property
    def pairsData(self):
        """
        DataFrame with the pairs data (groupId, winnerId, loserId and optionally weight).
        Can be None.
        """
        return self._call_java("pairsData")

    def quantize(self, quantizationParams = None):
        """Create Pool with quantized features from Pool with raw features"""
        if quantizationParams is None:
            quantizationParams = QuantizationParams()
        return self._call_java("quantize", quantizationParams)

    def repartition(self, partitionCount, byGroupColumnsIfPresent):
        """
        Repartion data to the specified number of partitions.
        Useful to repartition data to create one partition per executor for training
        (where each executor gets its' own CatBoost worker with a part of the training data).
        """
        return self._call_java("repartition", partitionCount, byGroupColumnsIfPresent)

    @staticmethod
    def load(sparkSession, dataPathWithScheme, columnDescription=None, poolLoadParams=None, pairsDataPathWithScheme=None):
        """
        Load dataset in one of CatBoost's natively supported formats:
           * dsv - https://catboost.ai/docs/concepts/input-data_values-file.html
           * libsvm - https://catboost.ai/docs/concepts/input-data_libsvm.html
        
        Parameters
        ----------
        sparkSession : SparkSession
        dataPathWithScheme : str
            Path with scheme to dataset in CatBoost format.
            For example, `dsv:///home/user/datasets/my_dataset/train.dsv` or
            `libsvm:///home/user/datasets/my_dataset/train.libsvm`
        columnDescription : str, optional
            Path to column description file. See https://catboost.ai/docs/concepts/input-data_column-descfile.html 
        params : PoolLoadParams, optional
            Additional params specifying data format.
        pairsDataPathWithScheme : str, optional
            Path with scheme to dataset pairs in CatBoost format.
            Only "dsv-grouped" format is supported for now.
            For example, `dsv-grouped:///home/user/datasets/my_dataset/train_pairs.dsv`
        
        Returns
        -------
           Pool
               Pool containing loaded data
        """
        if poolLoadParams is None:
            poolLoadParams = PoolLoadParams()
        sc = sparkSession.sparkContext
        java_obj = sc._jvm.ai.catboost.spark.Pool.load(
            _py2java(sc, sparkSession),
            dataPathWithScheme,
            (sc._jvm.java.nio.file.Paths.get(columnDescription, sc._gateway.new_array(sc._jvm.String, 0))
             if columnDescription
             else None
            ),
            _py2java(sc, poolLoadParams),
            pairsDataPathWithScheme
        )
        return Pool(java_obj)



class EAutoClassWeightsType(Enum):
    Balanced = 0
    SqrtBalanced = 1
    No = 2


class EBootstrapType(Enum):
    Poisson = 0
    Bayesian = 1
    Bernoulli = 2
    MVS = 3
    No = 4


class EBorderSelectionType(Enum):
    Median = 0
    GreedyLogSum = 1
    UniformAndQuantiles = 2
    MinEntropy = 3
    MaxLogSum = 4
    Uniform = 5
    GreedyMinEntropy = 6


class ECalcTypeShapValues(Enum):
    Approximate = 0
    Regular = 1
    Exact = 2
    Independent = 3


class EExplainableModelOutput(Enum):
    Raw = 0
    Probability = 1
    LossFunction = 2


class EFstrType(Enum):
    PredictionValuesChange = 0
    LossFunctionChange = 1
    FeatureImportance = 2
    InternalFeatureImportance = 3
    Interaction = 4
    InternalInteraction = 5
    ShapValues = 6
    PredictionDiff = 7
    ShapInteractionValues = 8


class ELeavesEstimation(Enum):
    Gradient = 0
    Newton = 1
    Exact = 2
    Simple = 3


class ELeavesEstimationStepBacktracking(Enum):
    No = 0
    AnyImprovement = 1
    Armijo = 2


class ELoggingLevel(Enum):
    Silent = 0
    Verbose = 1
    Info = 2
    Debug = 3


class EModelShrinkMode(Enum):
    Constant = 0
    Decreasing = 1


class EModelType(Enum):
    CatboostBinary = 0
    AppleCoreML = 1
    Cpp = 2
    Python = 3
    Json = 4
    Onnx = 5
    Pmml = 6
    CPUSnapshot = 7


class ENanMode(Enum):
    Min = 0
    Max = 1
    Forbidden = 2


class EOverfittingDetectorType(Enum):
    No = 0
    Wilcoxon = 1
    IncToDec = 2
    Iter = 3


class EPreCalcShapValues(Enum):
    Auto = 0
    UsePreCalc = 1
    NoPreCalc = 2


class ESamplingFrequency(Enum):
    PerTree = 0
    PerTreeLevel = 1


class ESamplingUnit(Enum):
    Object = 0
    Group = 1


class EScoreFunction(Enum):
    SolarL2 = 0
    Cosine = 1
    NewtonL2 = 2
    NewtonCosine = 3
    LOOL2 = 4
    SatL2 = 5
    L2 = 6



@inherit_doc
class CatBoostRegressor(JavaEstimator, MLReadable, JavaMLWritable):
    """
    Class to train CatBoostRegressionModel

    Init Parameters
    ---------------
        allowConstLabel : bool
            Use it to train models with datasets that have equal label values for all objects.
        allowWritingFiles : bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        approxOnFullHistory : bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        baggingTemperature : float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        bestModelMinTrees : int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        bootstrapType : EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        borderCount : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        connectTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=60000)
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        customMetric : list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        depth : int
            Depth of the tree.Default value is 6.
        diffusionTemperature : float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        earlyStoppingRounds : int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        evalMetric : str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        featureBorderType : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        featureWeightsList : list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        featureWeightsMap : dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        featuresCol : str, default: "features"
            features column name
        firstFeatureUsePenaltiesList : list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        firstFeatureUsePenaltiesMap : dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        foldLenMultiplier : float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        foldPermutationBlock : int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        hasTime : bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        ignoredFeaturesIndices : list
            Feature indices to exclude from the training
        ignoredFeaturesNames : list
            Feature names to exclude from the training
        inputBorders : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        iterations : int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        l2LeafReg : float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        labelCol : str, default: "label"
            label column name
        leafEstimationBacktracking : ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        leafEstimationIterations : int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        leafEstimationMethod : ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        learningRate : float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        loggingLevel : ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        lossFunction : str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        metricPeriod : int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        modelShrinkMode : EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        modelShrinkRate : float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        mvsReg : float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        nanMode : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        odPval : float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        odType : EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        odWait : int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        oneHotMaxSize : int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        penaltiesCoefficient : float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        perFloatFeatureQuantizaton : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        perObjectFeaturePenaltiesList : list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        perObjectFeaturePenaltiesMap : dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        predictionCol : str, default: "prediction"
            prediction column name
        randomSeed : int
            The random seed used for training. Default value is 0.
        randomStrength : float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        rsm : float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        samplingFrequency : ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        samplingUnit : ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        saveSnapshot : bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        scoreFunction : EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        snapshotFile : str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        snapshotInterval : datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        sparkPartitionCount : int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        subsample : float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        threadCount : int
            Number of CPU threads in parallel operations on client
        trainDir : str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        useBestModel : bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        weightCol : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        workerInitializationTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=600000)
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        workerMaxFailures : int, default: 4
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
    """

    @keyword_only
    def __init__(self, allowConstLabel=None, allowWritingFiles=None, approxOnFullHistory=None, baggingTemperature=None, bestModelMinTrees=None, bootstrapType=None, borderCount=None, connectTimeout=datetime.timedelta(milliseconds=60000), customMetric=None, depth=None, diffusionTemperature=None, earlyStoppingRounds=None, evalMetric=None, featureBorderType=None, featureWeightsList=None, featureWeightsMap=None, featuresCol="features", firstFeatureUsePenaltiesList=None, firstFeatureUsePenaltiesMap=None, foldLenMultiplier=None, foldPermutationBlock=None, hasTime=None, ignoredFeaturesIndices=None, ignoredFeaturesNames=None, inputBorders=None, iterations=None, l2LeafReg=None, labelCol="label", leafEstimationBacktracking=None, leafEstimationIterations=None, leafEstimationMethod=None, learningRate=None, loggingLevel=None, lossFunction=None, metricPeriod=None, modelShrinkMode=None, modelShrinkRate=None, mvsReg=None, nanMode=None, odPval=None, odType=None, odWait=None, oneHotMaxSize=None, penaltiesCoefficient=None, perFloatFeatureQuantizaton=None, perObjectFeaturePenaltiesList=None, perObjectFeaturePenaltiesMap=None, predictionCol="prediction", randomSeed=None, randomStrength=None, rsm=None, samplingFrequency=None, samplingUnit=None, saveSnapshot=None, scoreFunction=None, snapshotFile=None, snapshotInterval=None, sparkPartitionCount=None, subsample=None, threadCount=None, trainDir=None, useBestModel=None, weightCol=None, workerInitializationTimeout=datetime.timedelta(milliseconds=600000), workerMaxFailures=4):
        super(CatBoostRegressor, self).__init__()
        self._java_obj = self._new_java_obj("ai.catboost.spark.CatBoostRegressor")
        self.allowConstLabel = Param(self, "allowConstLabel", "Use it to train models with datasets that have equal label values for all objects.")
        self.allowWritingFiles = Param(self, "allowWritingFiles", "Allow to write analytical and snapshot files during training. Enabled by default.")
        self.approxOnFullHistory = Param(self, "approxOnFullHistory", "Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.")
        self.baggingTemperature = Param(self, "baggingTemperature", "This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.")
        self.bestModelMinTrees = Param(self, "bestModelMinTrees", "The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.")
        self.bootstrapType = Param(self, "bootstrapType", "Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.")
        self.borderCount = Param(self, "borderCount", "The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.")
        self.connectTimeout = Param(self, "connectTimeout", "Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute")
        self._setDefault(connectTimeout=datetime.timedelta(milliseconds=60000))
        self.customMetric = Param(self, "customMetric", "Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).")
        self.depth = Param(self, "depth", "Depth of the tree.Default value is 6.")
        self.diffusionTemperature = Param(self, "diffusionTemperature", "The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.")
        self.earlyStoppingRounds = Param(self, "earlyStoppingRounds", "Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.")
        self.evalMetric = Param(self, "evalMetric", "The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).")
        self.featureBorderType = Param(self, "featureBorderType", "The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'")
        self.featureWeightsList = Param(self, "featureWeightsList", "Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.")
        self.featureWeightsMap = Param(self, "featureWeightsMap", "Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.")
        self.featuresCol = Param(self, "featuresCol", "features column name")
        self._setDefault(featuresCol="features")
        self.firstFeatureUsePenaltiesList = Param(self, "firstFeatureUsePenaltiesList", "Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.")
        self.firstFeatureUsePenaltiesMap = Param(self, "firstFeatureUsePenaltiesMap", "Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.")
        self.foldLenMultiplier = Param(self, "foldLenMultiplier", "Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.")
        self.foldPermutationBlock = Param(self, "foldPermutationBlock", "Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.")
        self.hasTime = Param(self, "hasTime", "Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).")
        self.ignoredFeaturesIndices = Param(self, "ignoredFeaturesIndices", "Feature indices to exclude from the training")
        self.ignoredFeaturesNames = Param(self, "ignoredFeaturesNames", "Feature names to exclude from the training")
        self.inputBorders = Param(self, "inputBorders", "Load Custom quantization borders and missing value modes from a file (do not generate them)")
        self.iterations = Param(self, "iterations", "The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.")
        self.l2LeafReg = Param(self, "l2LeafReg", "Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.")
        self.labelCol = Param(self, "labelCol", "label column name")
        self._setDefault(labelCol="label")
        self.leafEstimationBacktracking = Param(self, "leafEstimationBacktracking", "When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'")
        self.leafEstimationIterations = Param(self, "leafEstimationIterations", "CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.")
        self.leafEstimationMethod = Param(self, "leafEstimationMethod", "The method used to calculate the values in leaves. See documentation for details.")
        self.learningRate = Param(self, "learningRate", "The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.")
        self.loggingLevel = Param(self, "loggingLevel", "The logging level to output to stdout. See documentation for details. Default value is 'Verbose'")
        self.lossFunction = Param(self, "lossFunction", "The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).")
        self.metricPeriod = Param(self, "metricPeriod", "The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.")
        self.modelShrinkMode = Param(self, "modelShrinkMode", "Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'")
        self.modelShrinkRate = Param(self, "modelShrinkRate", "The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.")
        self.mvsReg = Param(self, "mvsReg", "Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.")
        self.nanMode = Param(self, "nanMode", "The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'")
        self.odPval = Param(self, "odPval", "The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.")
        self.odType = Param(self, "odType", "The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'")
        self.odWait = Param(self, "odWait", "The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.")
        self.oneHotMaxSize = Param(self, "oneHotMaxSize", "Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.")
        self.penaltiesCoefficient = Param(self, "penaltiesCoefficient", "A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.")
        self.perFloatFeatureQuantizaton = Param(self, "perFloatFeatureQuantizaton", "The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]")
        self.perObjectFeaturePenaltiesList = Param(self, "perObjectFeaturePenaltiesList", "Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.")
        self.perObjectFeaturePenaltiesMap = Param(self, "perObjectFeaturePenaltiesMap", "Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.")
        self.predictionCol = Param(self, "predictionCol", "prediction column name")
        self._setDefault(predictionCol="prediction")
        self.randomSeed = Param(self, "randomSeed", "The random seed used for training. Default value is 0.")
        self.randomStrength = Param(self, "randomStrength", "The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0")
        self.rsm = Param(self, "rsm", "Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.")
        self.samplingFrequency = Param(self, "samplingFrequency", "Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'")
        self.samplingUnit = Param(self, "samplingUnit", "The sampling scheme, see documentation for details. Default value is 'Object'")
        self.saveSnapshot = Param(self, "saveSnapshot", "Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.")
        self.scoreFunction = Param(self, "scoreFunction", "The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'")
        self.snapshotFile = Param(self, "snapshotFile", "The name of the file to save the training progress information in. This file is used for recovering training after an interruption.")
        self.snapshotInterval = Param(self, "snapshotInterval", "The interval between saving snapshots. See documentation for details. Default value is 600 seconds.")
        self.sparkPartitionCount = Param(self, "sparkPartitionCount", "The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default")
        self.subsample = Param(self, "subsample", "Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.")
        self.threadCount = Param(self, "threadCount", "Number of CPU threads in parallel operations on client")
        self.trainDir = Param(self, "trainDir", "The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'")
        self.useBestModel = Param(self, "useBestModel", "If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.")
        self.weightCol = Param(self, "weightCol", "weight column name. If this is not set or empty, we treat all instance weights as 1.0")
        self.workerInitializationTimeout = Param(self, "workerInitializationTimeout", "Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes")
        self._setDefault(workerInitializationTimeout=datetime.timedelta(milliseconds=600000))
        self.workerMaxFailures = Param(self, "workerMaxFailures", "Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4")
        self._setDefault(workerMaxFailures=4)

        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)


    @keyword_only
    def setParams(self, allowConstLabel=None, allowWritingFiles=None, approxOnFullHistory=None, baggingTemperature=None, bestModelMinTrees=None, bootstrapType=None, borderCount=None, connectTimeout=datetime.timedelta(milliseconds=60000), customMetric=None, depth=None, diffusionTemperature=None, earlyStoppingRounds=None, evalMetric=None, featureBorderType=None, featureWeightsList=None, featureWeightsMap=None, featuresCol="features", firstFeatureUsePenaltiesList=None, firstFeatureUsePenaltiesMap=None, foldLenMultiplier=None, foldPermutationBlock=None, hasTime=None, ignoredFeaturesIndices=None, ignoredFeaturesNames=None, inputBorders=None, iterations=None, l2LeafReg=None, labelCol="label", leafEstimationBacktracking=None, leafEstimationIterations=None, leafEstimationMethod=None, learningRate=None, loggingLevel=None, lossFunction=None, metricPeriod=None, modelShrinkMode=None, modelShrinkRate=None, mvsReg=None, nanMode=None, odPval=None, odType=None, odWait=None, oneHotMaxSize=None, penaltiesCoefficient=None, perFloatFeatureQuantizaton=None, perObjectFeaturePenaltiesList=None, perObjectFeaturePenaltiesMap=None, predictionCol="prediction", randomSeed=None, randomStrength=None, rsm=None, samplingFrequency=None, samplingUnit=None, saveSnapshot=None, scoreFunction=None, snapshotFile=None, snapshotInterval=None, sparkPartitionCount=None, subsample=None, threadCount=None, trainDir=None, useBestModel=None, weightCol=None, workerInitializationTimeout=datetime.timedelta(milliseconds=600000), workerMaxFailures=4):
        """
        Set the (keyword only) parameters

        Parameters
        ----------
        allowConstLabel : bool
            Use it to train models with datasets that have equal label values for all objects.
        allowWritingFiles : bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        approxOnFullHistory : bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        baggingTemperature : float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        bestModelMinTrees : int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        bootstrapType : EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        borderCount : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        connectTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=60000)
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        customMetric : list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        depth : int
            Depth of the tree.Default value is 6.
        diffusionTemperature : float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        earlyStoppingRounds : int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        evalMetric : str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        featureBorderType : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        featureWeightsList : list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        featureWeightsMap : dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        featuresCol : str, default: "features"
            features column name
        firstFeatureUsePenaltiesList : list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        firstFeatureUsePenaltiesMap : dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        foldLenMultiplier : float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        foldPermutationBlock : int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        hasTime : bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        ignoredFeaturesIndices : list
            Feature indices to exclude from the training
        ignoredFeaturesNames : list
            Feature names to exclude from the training
        inputBorders : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        iterations : int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        l2LeafReg : float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        labelCol : str, default: "label"
            label column name
        leafEstimationBacktracking : ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        leafEstimationIterations : int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        leafEstimationMethod : ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        learningRate : float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        loggingLevel : ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        lossFunction : str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        metricPeriod : int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        modelShrinkMode : EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        modelShrinkRate : float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        mvsReg : float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        nanMode : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        odPval : float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        odType : EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        odWait : int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        oneHotMaxSize : int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        penaltiesCoefficient : float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        perFloatFeatureQuantizaton : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        perObjectFeaturePenaltiesList : list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        perObjectFeaturePenaltiesMap : dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        predictionCol : str, default: "prediction"
            prediction column name
        randomSeed : int
            The random seed used for training. Default value is 0.
        randomStrength : float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        rsm : float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        samplingFrequency : ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        samplingUnit : ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        saveSnapshot : bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        scoreFunction : EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        snapshotFile : str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        snapshotInterval : datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        sparkPartitionCount : int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        subsample : float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        threadCount : int
            Number of CPU threads in parallel operations on client
        trainDir : str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        useBestModel : bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        weightCol : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        workerInitializationTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=600000)
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        workerMaxFailures : int, default: 4
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)


    def getAllowConstLabel(self):
        """
        Returns
        -------
        bool
            Use it to train models with datasets that have equal label values for all objects.
        """
        return self.getOrDefault(self.allowConstLabel)

    def setAllowConstLabel(self, value):
        """
        Parameters
        ----------
        value : bool
            Use it to train models with datasets that have equal label values for all objects.
        """
        self._set(allowConstLabel=value)
        return self



    def getAllowWritingFiles(self):
        """
        Returns
        -------
        bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        """
        return self.getOrDefault(self.allowWritingFiles)

    def setAllowWritingFiles(self, value):
        """
        Parameters
        ----------
        value : bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        """
        self._set(allowWritingFiles=value)
        return self



    def getApproxOnFullHistory(self):
        """
        Returns
        -------
        bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        """
        return self.getOrDefault(self.approxOnFullHistory)

    def setApproxOnFullHistory(self, value):
        """
        Parameters
        ----------
        value : bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        """
        self._set(approxOnFullHistory=value)
        return self



    def getBaggingTemperature(self):
        """
        Returns
        -------
        float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        """
        return self.getOrDefault(self.baggingTemperature)

    def setBaggingTemperature(self, value):
        """
        Parameters
        ----------
        value : float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        """
        self._set(baggingTemperature=value)
        return self



    def getBestModelMinTrees(self):
        """
        Returns
        -------
        int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        """
        return self.getOrDefault(self.bestModelMinTrees)

    def setBestModelMinTrees(self, value):
        """
        Parameters
        ----------
        value : int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        """
        self._set(bestModelMinTrees=value)
        return self



    def getBootstrapType(self):
        """
        Returns
        -------
        EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        """
        return self.getOrDefault(self.bootstrapType)

    def setBootstrapType(self, value):
        """
        Parameters
        ----------
        value : EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        """
        self._set(bootstrapType=value)
        return self



    def getBorderCount(self):
        """
        Returns
        -------
        int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        """
        return self.getOrDefault(self.borderCount)

    def setBorderCount(self, value):
        """
        Parameters
        ----------
        value : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        """
        self._set(borderCount=value)
        return self



    def getConnectTimeout(self):
        """
        Returns
        -------
        datetime.timedelta
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        """
        return self.getOrDefault(self.connectTimeout)

    def setConnectTimeout(self, value):
        """
        Parameters
        ----------
        value : datetime.timedelta
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        """
        self._set(connectTimeout=value)
        return self



    def getCustomMetric(self):
        """
        Returns
        -------
        list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        """
        return self.getOrDefault(self.customMetric)

    def setCustomMetric(self, value):
        """
        Parameters
        ----------
        value : list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        """
        self._set(customMetric=value)
        return self



    def getDepth(self):
        """
        Returns
        -------
        int
            Depth of the tree.Default value is 6.
        """
        return self.getOrDefault(self.depth)

    def setDepth(self, value):
        """
        Parameters
        ----------
        value : int
            Depth of the tree.Default value is 6.
        """
        self._set(depth=value)
        return self



    def getDiffusionTemperature(self):
        """
        Returns
        -------
        float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        """
        return self.getOrDefault(self.diffusionTemperature)

    def setDiffusionTemperature(self, value):
        """
        Parameters
        ----------
        value : float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        """
        self._set(diffusionTemperature=value)
        return self



    def getEarlyStoppingRounds(self):
        """
        Returns
        -------
        int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        """
        return self.getOrDefault(self.earlyStoppingRounds)

    def setEarlyStoppingRounds(self, value):
        """
        Parameters
        ----------
        value : int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        """
        self._set(earlyStoppingRounds=value)
        return self



    def getEvalMetric(self):
        """
        Returns
        -------
        str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        return self.getOrDefault(self.evalMetric)

    def setEvalMetric(self, value):
        """
        Parameters
        ----------
        value : str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        self._set(evalMetric=value)
        return self



    def getFeatureBorderType(self):
        """
        Returns
        -------
        EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        """
        return self.getOrDefault(self.featureBorderType)

    def setFeatureBorderType(self, value):
        """
        Parameters
        ----------
        value : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        """
        self._set(featureBorderType=value)
        return self



    def getFeatureWeightsList(self):
        """
        Returns
        -------
        list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        """
        return self.getOrDefault(self.featureWeightsList)

    def setFeatureWeightsList(self, value):
        """
        Parameters
        ----------
        value : list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        """
        self._set(featureWeightsList=value)
        return self



    def getFeatureWeightsMap(self):
        """
        Returns
        -------
        dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        """
        return self.getOrDefault(self.featureWeightsMap)

    def setFeatureWeightsMap(self, value):
        """
        Parameters
        ----------
        value : dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        """
        self._set(featureWeightsMap=value)
        return self



    def getFeaturesCol(self):
        """
        Returns
        -------
        str
            features column name
        """
        return self.getOrDefault(self.featuresCol)

    def setFeaturesCol(self, value):
        """
        Parameters
        ----------
        value : str
            features column name
        """
        self._set(featuresCol=value)
        return self



    def getFirstFeatureUsePenaltiesList(self):
        """
        Returns
        -------
        list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        """
        return self.getOrDefault(self.firstFeatureUsePenaltiesList)

    def setFirstFeatureUsePenaltiesList(self, value):
        """
        Parameters
        ----------
        value : list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        """
        self._set(firstFeatureUsePenaltiesList=value)
        return self



    def getFirstFeatureUsePenaltiesMap(self):
        """
        Returns
        -------
        dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        """
        return self.getOrDefault(self.firstFeatureUsePenaltiesMap)

    def setFirstFeatureUsePenaltiesMap(self, value):
        """
        Parameters
        ----------
        value : dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        """
        self._set(firstFeatureUsePenaltiesMap=value)
        return self



    def getFoldLenMultiplier(self):
        """
        Returns
        -------
        float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        """
        return self.getOrDefault(self.foldLenMultiplier)

    def setFoldLenMultiplier(self, value):
        """
        Parameters
        ----------
        value : float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        """
        self._set(foldLenMultiplier=value)
        return self



    def getFoldPermutationBlock(self):
        """
        Returns
        -------
        int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        """
        return self.getOrDefault(self.foldPermutationBlock)

    def setFoldPermutationBlock(self, value):
        """
        Parameters
        ----------
        value : int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        """
        self._set(foldPermutationBlock=value)
        return self



    def getHasTime(self):
        """
        Returns
        -------
        bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        """
        return self.getOrDefault(self.hasTime)

    def setHasTime(self, value):
        """
        Parameters
        ----------
        value : bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        """
        self._set(hasTime=value)
        return self



    def getIgnoredFeaturesIndices(self):
        """
        Returns
        -------
        list
            Feature indices to exclude from the training
        """
        return self.getOrDefault(self.ignoredFeaturesIndices)

    def setIgnoredFeaturesIndices(self, value):
        """
        Parameters
        ----------
        value : list
            Feature indices to exclude from the training
        """
        self._set(ignoredFeaturesIndices=value)
        return self



    def getIgnoredFeaturesNames(self):
        """
        Returns
        -------
        list
            Feature names to exclude from the training
        """
        return self.getOrDefault(self.ignoredFeaturesNames)

    def setIgnoredFeaturesNames(self, value):
        """
        Parameters
        ----------
        value : list
            Feature names to exclude from the training
        """
        self._set(ignoredFeaturesNames=value)
        return self



    def getInputBorders(self):
        """
        Returns
        -------
        str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        """
        return self.getOrDefault(self.inputBorders)

    def setInputBorders(self, value):
        """
        Parameters
        ----------
        value : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        """
        self._set(inputBorders=value)
        return self



    def getIterations(self):
        """
        Returns
        -------
        int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        """
        return self.getOrDefault(self.iterations)

    def setIterations(self, value):
        """
        Parameters
        ----------
        value : int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        """
        self._set(iterations=value)
        return self



    def getL2LeafReg(self):
        """
        Returns
        -------
        float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        """
        return self.getOrDefault(self.l2LeafReg)

    def setL2LeafReg(self, value):
        """
        Parameters
        ----------
        value : float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        """
        self._set(l2LeafReg=value)
        return self



    def getLabelCol(self):
        """
        Returns
        -------
        str
            label column name
        """
        return self.getOrDefault(self.labelCol)

    def setLabelCol(self, value):
        """
        Parameters
        ----------
        value : str
            label column name
        """
        self._set(labelCol=value)
        return self



    def getLeafEstimationBacktracking(self):
        """
        Returns
        -------
        ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        """
        return self.getOrDefault(self.leafEstimationBacktracking)

    def setLeafEstimationBacktracking(self, value):
        """
        Parameters
        ----------
        value : ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        """
        self._set(leafEstimationBacktracking=value)
        return self



    def getLeafEstimationIterations(self):
        """
        Returns
        -------
        int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        """
        return self.getOrDefault(self.leafEstimationIterations)

    def setLeafEstimationIterations(self, value):
        """
        Parameters
        ----------
        value : int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        """
        self._set(leafEstimationIterations=value)
        return self



    def getLeafEstimationMethod(self):
        """
        Returns
        -------
        ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        """
        return self.getOrDefault(self.leafEstimationMethod)

    def setLeafEstimationMethod(self, value):
        """
        Parameters
        ----------
        value : ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        """
        self._set(leafEstimationMethod=value)
        return self



    def getLearningRate(self):
        """
        Returns
        -------
        float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        """
        return self.getOrDefault(self.learningRate)

    def setLearningRate(self, value):
        """
        Parameters
        ----------
        value : float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        """
        self._set(learningRate=value)
        return self



    def getLoggingLevel(self):
        """
        Returns
        -------
        ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        """
        return self.getOrDefault(self.loggingLevel)

    def setLoggingLevel(self, value):
        """
        Parameters
        ----------
        value : ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        """
        self._set(loggingLevel=value)
        return self



    def getLossFunction(self):
        """
        Returns
        -------
        str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        return self.getOrDefault(self.lossFunction)

    def setLossFunction(self, value):
        """
        Parameters
        ----------
        value : str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        self._set(lossFunction=value)
        return self



    def getMetricPeriod(self):
        """
        Returns
        -------
        int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        """
        return self.getOrDefault(self.metricPeriod)

    def setMetricPeriod(self, value):
        """
        Parameters
        ----------
        value : int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        """
        self._set(metricPeriod=value)
        return self



    def getModelShrinkMode(self):
        """
        Returns
        -------
        EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        """
        return self.getOrDefault(self.modelShrinkMode)

    def setModelShrinkMode(self, value):
        """
        Parameters
        ----------
        value : EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        """
        self._set(modelShrinkMode=value)
        return self



    def getModelShrinkRate(self):
        """
        Returns
        -------
        float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        """
        return self.getOrDefault(self.modelShrinkRate)

    def setModelShrinkRate(self, value):
        """
        Parameters
        ----------
        value : float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        """
        self._set(modelShrinkRate=value)
        return self



    def getMvsReg(self):
        """
        Returns
        -------
        float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        """
        return self.getOrDefault(self.mvsReg)

    def setMvsReg(self, value):
        """
        Parameters
        ----------
        value : float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        """
        self._set(mvsReg=value)
        return self



    def getNanMode(self):
        """
        Returns
        -------
        ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        """
        return self.getOrDefault(self.nanMode)

    def setNanMode(self, value):
        """
        Parameters
        ----------
        value : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        """
        self._set(nanMode=value)
        return self



    def getOdPval(self):
        """
        Returns
        -------
        float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        """
        return self.getOrDefault(self.odPval)

    def setOdPval(self, value):
        """
        Parameters
        ----------
        value : float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        """
        self._set(odPval=value)
        return self



    def getOdType(self):
        """
        Returns
        -------
        EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        """
        return self.getOrDefault(self.odType)

    def setOdType(self, value):
        """
        Parameters
        ----------
        value : EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        """
        self._set(odType=value)
        return self



    def getOdWait(self):
        """
        Returns
        -------
        int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        """
        return self.getOrDefault(self.odWait)

    def setOdWait(self, value):
        """
        Parameters
        ----------
        value : int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        """
        self._set(odWait=value)
        return self



    def getOneHotMaxSize(self):
        """
        Returns
        -------
        int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        """
        return self.getOrDefault(self.oneHotMaxSize)

    def setOneHotMaxSize(self, value):
        """
        Parameters
        ----------
        value : int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        """
        self._set(oneHotMaxSize=value)
        return self



    def getPenaltiesCoefficient(self):
        """
        Returns
        -------
        float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        """
        return self.getOrDefault(self.penaltiesCoefficient)

    def setPenaltiesCoefficient(self, value):
        """
        Parameters
        ----------
        value : float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        """
        self._set(penaltiesCoefficient=value)
        return self



    def getPerFloatFeatureQuantizaton(self):
        """
        Returns
        -------
        list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        """
        return self.getOrDefault(self.perFloatFeatureQuantizaton)

    def setPerFloatFeatureQuantizaton(self, value):
        """
        Parameters
        ----------
        value : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        """
        self._set(perFloatFeatureQuantizaton=value)
        return self



    def getPerObjectFeaturePenaltiesList(self):
        """
        Returns
        -------
        list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        """
        return self.getOrDefault(self.perObjectFeaturePenaltiesList)

    def setPerObjectFeaturePenaltiesList(self, value):
        """
        Parameters
        ----------
        value : list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        """
        self._set(perObjectFeaturePenaltiesList=value)
        return self



    def getPerObjectFeaturePenaltiesMap(self):
        """
        Returns
        -------
        dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        """
        return self.getOrDefault(self.perObjectFeaturePenaltiesMap)

    def setPerObjectFeaturePenaltiesMap(self, value):
        """
        Parameters
        ----------
        value : dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        """
        self._set(perObjectFeaturePenaltiesMap=value)
        return self



    def getPredictionCol(self):
        """
        Returns
        -------
        str
            prediction column name
        """
        return self.getOrDefault(self.predictionCol)

    def setPredictionCol(self, value):
        """
        Parameters
        ----------
        value : str
            prediction column name
        """
        self._set(predictionCol=value)
        return self



    def getRandomSeed(self):
        """
        Returns
        -------
        int
            The random seed used for training. Default value is 0.
        """
        return self.getOrDefault(self.randomSeed)

    def setRandomSeed(self, value):
        """
        Parameters
        ----------
        value : int
            The random seed used for training. Default value is 0.
        """
        self._set(randomSeed=value)
        return self



    def getRandomStrength(self):
        """
        Returns
        -------
        float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        """
        return self.getOrDefault(self.randomStrength)

    def setRandomStrength(self, value):
        """
        Parameters
        ----------
        value : float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        """
        self._set(randomStrength=value)
        return self



    def getRsm(self):
        """
        Returns
        -------
        float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        """
        return self.getOrDefault(self.rsm)

    def setRsm(self, value):
        """
        Parameters
        ----------
        value : float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        """
        self._set(rsm=value)
        return self



    def getSamplingFrequency(self):
        """
        Returns
        -------
        ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        """
        return self.getOrDefault(self.samplingFrequency)

    def setSamplingFrequency(self, value):
        """
        Parameters
        ----------
        value : ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        """
        self._set(samplingFrequency=value)
        return self



    def getSamplingUnit(self):
        """
        Returns
        -------
        ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        """
        return self.getOrDefault(self.samplingUnit)

    def setSamplingUnit(self, value):
        """
        Parameters
        ----------
        value : ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        """
        self._set(samplingUnit=value)
        return self



    def getSaveSnapshot(self):
        """
        Returns
        -------
        bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        """
        return self.getOrDefault(self.saveSnapshot)

    def setSaveSnapshot(self, value):
        """
        Parameters
        ----------
        value : bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        """
        self._set(saveSnapshot=value)
        return self



    def getScoreFunction(self):
        """
        Returns
        -------
        EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        """
        return self.getOrDefault(self.scoreFunction)

    def setScoreFunction(self, value):
        """
        Parameters
        ----------
        value : EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        """
        self._set(scoreFunction=value)
        return self



    def getSnapshotFile(self):
        """
        Returns
        -------
        str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        """
        return self.getOrDefault(self.snapshotFile)

    def setSnapshotFile(self, value):
        """
        Parameters
        ----------
        value : str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        """
        self._set(snapshotFile=value)
        return self



    def getSnapshotInterval(self):
        """
        Returns
        -------
        datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        """
        return self.getOrDefault(self.snapshotInterval)

    def setSnapshotInterval(self, value):
        """
        Parameters
        ----------
        value : datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        """
        self._set(snapshotInterval=value)
        return self



    def getSparkPartitionCount(self):
        """
        Returns
        -------
        int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        """
        return self.getOrDefault(self.sparkPartitionCount)

    def setSparkPartitionCount(self, value):
        """
        Parameters
        ----------
        value : int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        """
        self._set(sparkPartitionCount=value)
        return self



    def getSubsample(self):
        """
        Returns
        -------
        float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        """
        return self.getOrDefault(self.subsample)

    def setSubsample(self, value):
        """
        Parameters
        ----------
        value : float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        """
        self._set(subsample=value)
        return self



    def getThreadCount(self):
        """
        Returns
        -------
        int
            Number of CPU threads in parallel operations on client
        """
        return self.getOrDefault(self.threadCount)

    def setThreadCount(self, value):
        """
        Parameters
        ----------
        value : int
            Number of CPU threads in parallel operations on client
        """
        self._set(threadCount=value)
        return self



    def getTrainDir(self):
        """
        Returns
        -------
        str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        """
        return self.getOrDefault(self.trainDir)

    def setTrainDir(self, value):
        """
        Parameters
        ----------
        value : str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        """
        self._set(trainDir=value)
        return self



    def getUseBestModel(self):
        """
        Returns
        -------
        bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        """
        return self.getOrDefault(self.useBestModel)

    def setUseBestModel(self, value):
        """
        Parameters
        ----------
        value : bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        """
        self._set(useBestModel=value)
        return self



    def getWeightCol(self):
        """
        Returns
        -------
        str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        """
        return self.getOrDefault(self.weightCol)

    def setWeightCol(self, value):
        """
        Parameters
        ----------
        value : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        """
        self._set(weightCol=value)
        return self



    def getWorkerInitializationTimeout(self):
        """
        Returns
        -------
        datetime.timedelta
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        """
        return self.getOrDefault(self.workerInitializationTimeout)

    def setWorkerInitializationTimeout(self, value):
        """
        Parameters
        ----------
        value : datetime.timedelta
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        """
        self._set(workerInitializationTimeout=value)
        return self



    def getWorkerMaxFailures(self):
        """
        Returns
        -------
        int
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
        """
        return self.getOrDefault(self.workerMaxFailures)

    def setWorkerMaxFailures(self, value):
        """
        Parameters
        ----------
        value : int
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
        """
        self._set(workerMaxFailures=value)
        return self




    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return CatBoostMLReader(cls)

    def _create_model(self, java_model):
        return CatBoostRegressionModel(java_model)

    def _fit_with_eval(self, trainDatasetAsJavaObject, evalDatasetsAsJavaObject, params=None):
        """
        Implementation of fit with eval datasets with no more than one set of optional parameters
        """
        if params:
            return self.copy(params)._fit_with_eval(trainDatasetAsJavaObject, evalDatasetsAsJavaObject)
        else:
            self._transfer_params_to_java()
            java_model = self._java_obj.fit(trainDatasetAsJavaObject, evalDatasetsAsJavaObject)
            return CatBoostRegressionModel(java_model)

    def fit(self, dataset, params=None, evalDatasets=None):
        """
        Extended variant of standard Estimator's fit method
        that accepts CatBoost's Pool s and allows to specify additional
        datasets for computing evaluation metrics and overfitting detection similarily to CatBoost's other APIs.
        
        Parameters
        ---------- 
        dataset : Pool or DataFrame
          The input training dataset.
        params : dict or list or tuple, optional
          an optional param map that overrides embedded params. If a list/tuple of
          param maps is given, this calls fit on each param map and returns a list of
          models.
        evalDatasets : Pools, optional
          The validation datasets used for the following processes:
           - overfitting detector
           - best iteration selection
           - monitoring metrics' changes
        
        Returns
        -------
        trained model(s): CatBoostRegressionModel or a list of trained CatBoostRegressionModel
        """
        if (isinstance(dataset, DataFrame)):
            if evalDatasets is not None:
                raise RuntimeError("if dataset has type DataFrame no evalDatasets are supported")
            return JavaEstimator.fit(self, dataset, params)
        else:
            sc = SparkContext._active_spark_context

            trainDatasetAsJavaObject = _py2java(sc, dataset)
            evalDatasetCount = 0 if (evalDatasets is None) else len(evalDatasets)

            # need to create it because default mapping for python list is ArrayList, not Array
            evalDatasetsAsJavaObject = sc._gateway.new_array(sc._jvm.ai.catboost.spark.Pool, evalDatasetCount)
            for i in range(evalDatasetCount):
                evalDatasetsAsJavaObject[i] = _py2java(sc, evalDatasets[i])

            def _fit_with_eval(params):
                return self._fit_with_eval(trainDatasetAsJavaObject, evalDatasetsAsJavaObject, params)

            if (params is None) or isinstance(params, dict):
                return _fit_with_eval(params)
            if isinstance(params, (list, tuple)):
                models = []
                for paramsInstance in params:
                    models.append(_fit_with_eval(paramsInstance))
                return models
            else:
                raise TypeError("Params must be either a param map or a list/tuple of param maps, "
                                "but got %s." % type(params))

@inherit_doc
class CatBoostRegressionModel(_JavaRegressionModel, MLReadable, JavaMLWritable):
    """
    Regression model trained by CatBoost. Use CatBoostRegressor to train it
    """
    def __init__(self, java_model=None):
        super(CatBoostRegressionModel, self).__init__(java_model)
        self.featuresCol = Param(self, "featuresCol", "features column name")
        self._setDefault(featuresCol="features")
        self.labelCol = Param(self, "labelCol", "label column name")
        self._setDefault(labelCol="label")
        self.predictionCol = Param(self, "predictionCol", "prediction column name")
        self._setDefault(predictionCol="prediction")
        if java_model is not None:
            self._transfer_params_from_java()


    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction"):
        """
        Set the (keyword only) parameters

        Parameters
        ----------
        featuresCol : str, default: "features"
            features column name
        labelCol : str, default: "label"
            label column name
        predictionCol : str, default: "prediction"
            prediction column name
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)


    def getFeaturesCol(self):
        """
        Returns
        -------
        str
            features column name
        """
        return self.getOrDefault(self.featuresCol)

    def setFeaturesCol(self, value):
        """
        Parameters
        ----------
        value : str
            features column name
        """
        self._set(featuresCol=value)
        return self



    def getLabelCol(self):
        """
        Returns
        -------
        str
            label column name
        """
        return self.getOrDefault(self.labelCol)

    def setLabelCol(self, value):
        """
        Parameters
        ----------
        value : str
            label column name
        """
        self._set(labelCol=value)
        return self



    def getPredictionCol(self):
        """
        Returns
        -------
        str
            prediction column name
        """
        return self.getOrDefault(self.predictionCol)

    def setPredictionCol(self, value):
        """
        Parameters
        ----------
        value : str
            prediction column name
        """
        self._set(predictionCol=value)
        return self




    @staticmethod
    def _from_java(java_model):
        return CatBoostRegressionModel(java_model)

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return CatBoostMLReader(cls)

    def saveNativeModel(self, fileName, format=EModelType.CatboostBinary, exportParameters=None, pool=None):
        """
        Save the model to a local file.
        See https://catboost.ai/docs/concepts/python-reference_catboostclassifier_save_model.html
        for detailed parameters description
        """
        return self._call_java("saveNativeModel", fileName, format, exportParameters, pool)

    @staticmethod
    def loadNativeModel(fileName, format=EModelType.CatboostBinary):
        """
        Load the model from a local file.
        See https://catboost.ai/docs/concepts/python-reference_catboostclassifier_load_model.html
        for detailed parameters description
        """
        sc = SparkContext._active_spark_context
        java_model = sc._jvm.ai.catboost.spark.CatBoostRegressionModel.loadNativeModel(fileName, _py2java(sc, format))
        return CatBoostRegressionModel(java_model)


    def transformPool(self, pool):
        """
        This function is useful when the dataset has been already quantized but works with any Pool
        """
        return self._call_java("transformPool", pool)


    def getFeatureImportance(self, 
                             fstrType=EFstrType.FeatureImportance,
                             data=None,
                             calcType=ECalcTypeShapValues.Regular
                            ):
        """
        Parameters
        ----------
        fstrType : EFstrType
            Supported values are FeatureImportance, PredictionValuesChange, LossFunctionChange, PredictionDiff
        data : Pool
            if fstrType is PredictionDiff it is required and must contain 2 samples
            if fstrType is PredictionValuesChange this param is required in case if model was explicitly trained
            with flag to store no leaf weights.
            otherwise it can be null
        calcType : ECalcTypeShapValues
            Used only for PredictionValuesChange. 
            Possible values:
              - Regular
                 Calculate regular SHAP values
              - Approximate
                 Calculate approximate SHAP values
              - Exact
                 Calculate exact SHAP values

        Returns
        -------
        list of float
            array of feature importances (index corresponds to the order of features in the model)
        """
        return self._call_java("getFeatureImportance", fstrType, data, calcType)

    def getFeatureImportancePrettified(self, 
                                       fstrType=EFstrType.FeatureImportance,
                                       data=None,
                                       calcType=ECalcTypeShapValues.Regular
                                      ):
        """
        Parameters
        ----------
        fstrType : EFstrType
            Supported values are FeatureImportance, PredictionValuesChange, LossFunctionChange, PredictionDiff
        data : Pool
            if fstrType is PredictionDiff it is required and must contain 2 samples
            if fstrType is PredictionValuesChange this param is required in case if model was explicitly trained
            with flag to store no leaf weights.
            otherwise it can be null
        calcType : ECalcTypeShapValues
            Used only for PredictionValuesChange. 
            Possible values:

              - Regular
                 Calculate regular SHAP values
              - Approximate
                 Calculate approximate SHAP values
              - Exact
                 Calculate exact SHAP values

        Returns
        -------
        list of FeatureImportance
            array of feature importances sorted in descending order by importance
        """
        return self._call_java("getFeatureImportancePrettified", fstrType, data, calcType)

    def getFeatureImportanceShapValues(self,
                                       data,
                                       preCalcMode=EPreCalcShapValues.Auto,
                                       calcType=ECalcTypeShapValues.Regular,
                                       modelOutputType=EExplainableModelOutput.Raw,
                                       referenceData=None,
                                       outputColumns=None
                                      ):
        """
        Parameters
        ----------
        data : Pool
            dataset to calculate SHAP values for
        preCalcMode : EPreCalcShapValues
            Possible values:
               - Auto
                  Use direct SHAP Values calculation only if data size is smaller than average leaves number
                  (the best of two strategies below is chosen).
               - UsePreCalc
                  Calculate SHAP Values for every leaf in preprocessing. Final complexity is
                  O(NT(D+F))+O(TL^2 D^2) where N is the number of documents(objects), T - number of trees,
                  D - average tree depth, F - average number of features in tree, L - average number of leaves in tree
                  This is much faster (because of a smaller constant) than direct calculation when N >> L
               - NoPreCalc
                  Use direct SHAP Values calculation calculation with complexity O(NTLD^2). Direct algorithm
                  is faster when N < L (algorithm from https://arxiv.org/abs/1802.03888)
        calcType : ECalcTypeShapValues
            Possible values:

              - Regular
                 Calculate regular SHAP values
              - Approximate
                 Calculate approximate SHAP values
              - Exact
                 Calculate exact SHAP values
        referenceData : Pool
            reference data for Independent Tree SHAP values from https://arxiv.org/abs/1905.04610v1
            if referenceData is not null, then Independent Tree SHAP values are calculated
        outputColumns : list of str
            columns from data to add to output DataFrame, if None - add all columns

        Returns
        -------
        DataFrame
            - for regression and binclass models: 
              contains outputColumns and "shapValues" column with Vector of length (n_features + 1) with SHAP values
            - for multiclass models:
              contains outputColumns and "shapValues" column with Matrix of shape (n_classes x (n_features + 1)) with SHAP values
        """
        return self._call_java(
            "getFeatureImportanceShapValues", 
            data, 
            preCalcMode,
            calcType,
            modelOutputType,
            referenceData,
            outputColumns
        )

    def getFeatureImportanceShapInteractionValues(self,
                                                  data,
                                                  featureIndices=None,
                                                  featureNames=None,
                                                  preCalcMode=EPreCalcShapValues.Auto,
                                                  calcType=ECalcTypeShapValues.Regular,
                                                  outputColumns=None):
        """
        SHAP interaction values are calculated for all features pairs if nor featureIndices nor featureNames 
          are specified.

        Parameters
        ----------
        data : Pool
            dataset to calculate SHAP interaction values
        featureIndices : (int, int), optional
            pair of features indices to calculate SHAP interaction values for.
        featureNames : (str, str), optional
            pair of features names to calculate SHAP interaction values for.
        preCalcMode : EPreCalcShapValues
            Possible values:

            - Auto
                Use direct SHAP Values calculation only if data size is smaller than average leaves number
                (the best of two strategies below is chosen).
            - UsePreCalc
                Calculate SHAP Values for every leaf in preprocessing. Final complexity is
                O(NT(D+F))+O(TL^2 D^2) where N is the number of documents(objects), T - number of trees,
                D - average tree depth, F - average number of features in tree, L - average number of leaves in tree
                This is much faster (because of a smaller constant) than direct calculation when N >> L
            - NoPreCalc
                Use direct SHAP Values calculation calculation with complexity O(NTLD^2). Direct algorithm
                is faster when N < L (algorithm from https://arxiv.org/abs/1802.03888)
        calcType : ECalcTypeShapValues
            Possible values:

              - Regular
                  Calculate regular SHAP values
              - Approximate
                  Calculate approximate SHAP values
              - Exact
                  Calculate exact SHAP values
        outputColumns : list of str
            columns from data to add to output DataFrame, if None - add all columns

        Returns
        -------
        DataFrame
            - for regression and binclass models: 
              contains outputColumns and "featureIdx1", "featureIdx2", "shapInteractionValue" columns
            - for multiclass models:
              contains outputColumns and "classIdx", "featureIdx1", "featureIdx2", "shapInteractionValue" columns
        """
        return self._call_java(
            "getFeatureImportanceShapInteractionValues", 
            data,
            featureIndices,
            featureNames,
            preCalcMode, 
            calcType,
            outputColumns
        )

    def getFeatureImportanceInteraction(self):
        """
        Returns
        -------
        list of FeatureInteractionScore
        """
        return self._call_java("getFeatureImportanceInteraction")




@inherit_doc
class CatBoostClassifier(JavaEstimator, MLReadable, JavaMLWritable):
    """
    Class to train CatBoostClassificationModel

    Init Parameters
    ---------------
        allowConstLabel : bool
            Use it to train models with datasets that have equal label values for all objects.
        allowWritingFiles : bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        approxOnFullHistory : bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        autoClassWeights : EAutoClassWeightsType
            Automatically calculate class weights based either on the total weight or the total number of objects in each class. The values are used as multipliers for the object weights. Default value is 'None'
        baggingTemperature : float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        bestModelMinTrees : int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        bootstrapType : EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        borderCount : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        classNames : list
            Allows to redefine the default values (consecutive integers).
        classWeightsList : list
            List of weights for each class. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsMap.
        classWeightsMap : dict
            Map from class name to weight. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsList.
        classesCount : int
            The upper limit for the numeric class label. Defines the number of classes for multiclassification. See documentation for details.
        connectTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=60000)
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        customMetric : list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        depth : int
            Depth of the tree.Default value is 6.
        diffusionTemperature : float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        earlyStoppingRounds : int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        evalMetric : str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        featureBorderType : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        featureWeightsList : list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        featureWeightsMap : dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        featuresCol : str, default: "features"
            features column name
        firstFeatureUsePenaltiesList : list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        firstFeatureUsePenaltiesMap : dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        foldLenMultiplier : float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        foldPermutationBlock : int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        hasTime : bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        ignoredFeaturesIndices : list
            Feature indices to exclude from the training
        ignoredFeaturesNames : list
            Feature names to exclude from the training
        inputBorders : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        iterations : int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        l2LeafReg : float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        labelCol : str, default: "label"
            label column name
        leafEstimationBacktracking : ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        leafEstimationIterations : int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        leafEstimationMethod : ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        learningRate : float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        loggingLevel : ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        lossFunction : str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        metricPeriod : int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        modelShrinkMode : EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        modelShrinkRate : float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        mvsReg : float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        nanMode : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        odPval : float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        odType : EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        odWait : int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        oneHotMaxSize : int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        penaltiesCoefficient : float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        perFloatFeatureQuantizaton : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        perObjectFeaturePenaltiesList : list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        perObjectFeaturePenaltiesMap : dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        predictionCol : str, default: "prediction"
            prediction column name
        probabilityCol : str, default: "probability"
            Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        randomSeed : int
            The random seed used for training. Default value is 0.
        randomStrength : float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        rawPredictionCol : str, default: "rawPrediction"
            raw prediction (a.k.a. confidence) column name
        rsm : float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        samplingFrequency : ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        samplingUnit : ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        saveSnapshot : bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        scalePosWeight : float
            The weight for class 1 in binary classification. The value is used as a multiplier for the weights of objects from class 1. Default value is 1 (both classes have equal weight).
        scoreFunction : EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        snapshotFile : str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        snapshotInterval : datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        sparkPartitionCount : int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        subsample : float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        targetBorder : float
            If set, defines the border for converting target values to 0 and 1 classes.
        threadCount : int
            Number of CPU threads in parallel operations on client
        thresholds : list
            Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        trainDir : str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        useBestModel : bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        weightCol : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        workerInitializationTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=600000)
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        workerMaxFailures : int, default: 4
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
    """

    @keyword_only
    def __init__(self, allowConstLabel=None, allowWritingFiles=None, approxOnFullHistory=None, autoClassWeights=None, baggingTemperature=None, bestModelMinTrees=None, bootstrapType=None, borderCount=None, classNames=None, classWeightsList=None, classWeightsMap=None, classesCount=None, connectTimeout=datetime.timedelta(milliseconds=60000), customMetric=None, depth=None, diffusionTemperature=None, earlyStoppingRounds=None, evalMetric=None, featureBorderType=None, featureWeightsList=None, featureWeightsMap=None, featuresCol="features", firstFeatureUsePenaltiesList=None, firstFeatureUsePenaltiesMap=None, foldLenMultiplier=None, foldPermutationBlock=None, hasTime=None, ignoredFeaturesIndices=None, ignoredFeaturesNames=None, inputBorders=None, iterations=None, l2LeafReg=None, labelCol="label", leafEstimationBacktracking=None, leafEstimationIterations=None, leafEstimationMethod=None, learningRate=None, loggingLevel=None, lossFunction=None, metricPeriod=None, modelShrinkMode=None, modelShrinkRate=None, mvsReg=None, nanMode=None, odPval=None, odType=None, odWait=None, oneHotMaxSize=None, penaltiesCoefficient=None, perFloatFeatureQuantizaton=None, perObjectFeaturePenaltiesList=None, perObjectFeaturePenaltiesMap=None, predictionCol="prediction", probabilityCol="probability", randomSeed=None, randomStrength=None, rawPredictionCol="rawPrediction", rsm=None, samplingFrequency=None, samplingUnit=None, saveSnapshot=None, scalePosWeight=None, scoreFunction=None, snapshotFile=None, snapshotInterval=None, sparkPartitionCount=None, subsample=None, targetBorder=None, threadCount=None, thresholds=None, trainDir=None, useBestModel=None, weightCol=None, workerInitializationTimeout=datetime.timedelta(milliseconds=600000), workerMaxFailures=4):
        super(CatBoostClassifier, self).__init__()
        self._java_obj = self._new_java_obj("ai.catboost.spark.CatBoostClassifier")
        self.allowConstLabel = Param(self, "allowConstLabel", "Use it to train models with datasets that have equal label values for all objects.")
        self.allowWritingFiles = Param(self, "allowWritingFiles", "Allow to write analytical and snapshot files during training. Enabled by default.")
        self.approxOnFullHistory = Param(self, "approxOnFullHistory", "Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.")
        self.autoClassWeights = Param(self, "autoClassWeights", "Automatically calculate class weights based either on the total weight or the total number of objects in each class. The values are used as multipliers for the object weights. Default value is 'None'")
        self.baggingTemperature = Param(self, "baggingTemperature", "This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.")
        self.bestModelMinTrees = Param(self, "bestModelMinTrees", "The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.")
        self.bootstrapType = Param(self, "bootstrapType", "Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.")
        self.borderCount = Param(self, "borderCount", "The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.")
        self.classNames = Param(self, "classNames", "Allows to redefine the default values (consecutive integers).")
        self.classWeightsList = Param(self, "classWeightsList", "List of weights for each class. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsMap.")
        self.classWeightsMap = Param(self, "classWeightsMap", "Map from class name to weight. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsList.")
        self.classesCount = Param(self, "classesCount", "The upper limit for the numeric class label. Defines the number of classes for multiclassification. See documentation for details.")
        self.connectTimeout = Param(self, "connectTimeout", "Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute")
        self._setDefault(connectTimeout=datetime.timedelta(milliseconds=60000))
        self.customMetric = Param(self, "customMetric", "Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).")
        self.depth = Param(self, "depth", "Depth of the tree.Default value is 6.")
        self.diffusionTemperature = Param(self, "diffusionTemperature", "The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.")
        self.earlyStoppingRounds = Param(self, "earlyStoppingRounds", "Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.")
        self.evalMetric = Param(self, "evalMetric", "The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).")
        self.featureBorderType = Param(self, "featureBorderType", "The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'")
        self.featureWeightsList = Param(self, "featureWeightsList", "Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.")
        self.featureWeightsMap = Param(self, "featureWeightsMap", "Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.")
        self.featuresCol = Param(self, "featuresCol", "features column name")
        self._setDefault(featuresCol="features")
        self.firstFeatureUsePenaltiesList = Param(self, "firstFeatureUsePenaltiesList", "Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.")
        self.firstFeatureUsePenaltiesMap = Param(self, "firstFeatureUsePenaltiesMap", "Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.")
        self.foldLenMultiplier = Param(self, "foldLenMultiplier", "Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.")
        self.foldPermutationBlock = Param(self, "foldPermutationBlock", "Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.")
        self.hasTime = Param(self, "hasTime", "Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).")
        self.ignoredFeaturesIndices = Param(self, "ignoredFeaturesIndices", "Feature indices to exclude from the training")
        self.ignoredFeaturesNames = Param(self, "ignoredFeaturesNames", "Feature names to exclude from the training")
        self.inputBorders = Param(self, "inputBorders", "Load Custom quantization borders and missing value modes from a file (do not generate them)")
        self.iterations = Param(self, "iterations", "The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.")
        self.l2LeafReg = Param(self, "l2LeafReg", "Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.")
        self.labelCol = Param(self, "labelCol", "label column name")
        self._setDefault(labelCol="label")
        self.leafEstimationBacktracking = Param(self, "leafEstimationBacktracking", "When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'")
        self.leafEstimationIterations = Param(self, "leafEstimationIterations", "CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.")
        self.leafEstimationMethod = Param(self, "leafEstimationMethod", "The method used to calculate the values in leaves. See documentation for details.")
        self.learningRate = Param(self, "learningRate", "The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.")
        self.loggingLevel = Param(self, "loggingLevel", "The logging level to output to stdout. See documentation for details. Default value is 'Verbose'")
        self.lossFunction = Param(self, "lossFunction", "The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).")
        self.metricPeriod = Param(self, "metricPeriod", "The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.")
        self.modelShrinkMode = Param(self, "modelShrinkMode", "Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'")
        self.modelShrinkRate = Param(self, "modelShrinkRate", "The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.")
        self.mvsReg = Param(self, "mvsReg", "Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.")
        self.nanMode = Param(self, "nanMode", "The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'")
        self.odPval = Param(self, "odPval", "The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.")
        self.odType = Param(self, "odType", "The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'")
        self.odWait = Param(self, "odWait", "The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.")
        self.oneHotMaxSize = Param(self, "oneHotMaxSize", "Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.")
        self.penaltiesCoefficient = Param(self, "penaltiesCoefficient", "A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.")
        self.perFloatFeatureQuantizaton = Param(self, "perFloatFeatureQuantizaton", "The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]")
        self.perObjectFeaturePenaltiesList = Param(self, "perObjectFeaturePenaltiesList", "Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.")
        self.perObjectFeaturePenaltiesMap = Param(self, "perObjectFeaturePenaltiesMap", "Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.")
        self.predictionCol = Param(self, "predictionCol", "prediction column name")
        self._setDefault(predictionCol="prediction")
        self.probabilityCol = Param(self, "probabilityCol", "Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities")
        self._setDefault(probabilityCol="probability")
        self.randomSeed = Param(self, "randomSeed", "The random seed used for training. Default value is 0.")
        self.randomStrength = Param(self, "randomStrength", "The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0")
        self.rawPredictionCol = Param(self, "rawPredictionCol", "raw prediction (a.k.a. confidence) column name")
        self._setDefault(rawPredictionCol="rawPrediction")
        self.rsm = Param(self, "rsm", "Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.")
        self.samplingFrequency = Param(self, "samplingFrequency", "Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'")
        self.samplingUnit = Param(self, "samplingUnit", "The sampling scheme, see documentation for details. Default value is 'Object'")
        self.saveSnapshot = Param(self, "saveSnapshot", "Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.")
        self.scalePosWeight = Param(self, "scalePosWeight", "The weight for class 1 in binary classification. The value is used as a multiplier for the weights of objects from class 1. Default value is 1 (both classes have equal weight).")
        self.scoreFunction = Param(self, "scoreFunction", "The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'")
        self.snapshotFile = Param(self, "snapshotFile", "The name of the file to save the training progress information in. This file is used for recovering training after an interruption.")
        self.snapshotInterval = Param(self, "snapshotInterval", "The interval between saving snapshots. See documentation for details. Default value is 600 seconds.")
        self.sparkPartitionCount = Param(self, "sparkPartitionCount", "The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default")
        self.subsample = Param(self, "subsample", "Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.")
        self.targetBorder = Param(self, "targetBorder", "If set, defines the border for converting target values to 0 and 1 classes.")
        self.threadCount = Param(self, "threadCount", "Number of CPU threads in parallel operations on client")
        self.thresholds = Param(self, "thresholds", "Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold")
        self.trainDir = Param(self, "trainDir", "The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'")
        self.useBestModel = Param(self, "useBestModel", "If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.")
        self.weightCol = Param(self, "weightCol", "weight column name. If this is not set or empty, we treat all instance weights as 1.0")
        self.workerInitializationTimeout = Param(self, "workerInitializationTimeout", "Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes")
        self._setDefault(workerInitializationTimeout=datetime.timedelta(milliseconds=600000))
        self.workerMaxFailures = Param(self, "workerMaxFailures", "Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4")
        self._setDefault(workerMaxFailures=4)

        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)


    @keyword_only
    def setParams(self, allowConstLabel=None, allowWritingFiles=None, approxOnFullHistory=None, autoClassWeights=None, baggingTemperature=None, bestModelMinTrees=None, bootstrapType=None, borderCount=None, classNames=None, classWeightsList=None, classWeightsMap=None, classesCount=None, connectTimeout=datetime.timedelta(milliseconds=60000), customMetric=None, depth=None, diffusionTemperature=None, earlyStoppingRounds=None, evalMetric=None, featureBorderType=None, featureWeightsList=None, featureWeightsMap=None, featuresCol="features", firstFeatureUsePenaltiesList=None, firstFeatureUsePenaltiesMap=None, foldLenMultiplier=None, foldPermutationBlock=None, hasTime=None, ignoredFeaturesIndices=None, ignoredFeaturesNames=None, inputBorders=None, iterations=None, l2LeafReg=None, labelCol="label", leafEstimationBacktracking=None, leafEstimationIterations=None, leafEstimationMethod=None, learningRate=None, loggingLevel=None, lossFunction=None, metricPeriod=None, modelShrinkMode=None, modelShrinkRate=None, mvsReg=None, nanMode=None, odPval=None, odType=None, odWait=None, oneHotMaxSize=None, penaltiesCoefficient=None, perFloatFeatureQuantizaton=None, perObjectFeaturePenaltiesList=None, perObjectFeaturePenaltiesMap=None, predictionCol="prediction", probabilityCol="probability", randomSeed=None, randomStrength=None, rawPredictionCol="rawPrediction", rsm=None, samplingFrequency=None, samplingUnit=None, saveSnapshot=None, scalePosWeight=None, scoreFunction=None, snapshotFile=None, snapshotInterval=None, sparkPartitionCount=None, subsample=None, targetBorder=None, threadCount=None, thresholds=None, trainDir=None, useBestModel=None, weightCol=None, workerInitializationTimeout=datetime.timedelta(milliseconds=600000), workerMaxFailures=4):
        """
        Set the (keyword only) parameters

        Parameters
        ----------
        allowConstLabel : bool
            Use it to train models with datasets that have equal label values for all objects.
        allowWritingFiles : bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        approxOnFullHistory : bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        autoClassWeights : EAutoClassWeightsType
            Automatically calculate class weights based either on the total weight or the total number of objects in each class. The values are used as multipliers for the object weights. Default value is 'None'
        baggingTemperature : float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        bestModelMinTrees : int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        bootstrapType : EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        borderCount : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        classNames : list
            Allows to redefine the default values (consecutive integers).
        classWeightsList : list
            List of weights for each class. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsMap.
        classWeightsMap : dict
            Map from class name to weight. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsList.
        classesCount : int
            The upper limit for the numeric class label. Defines the number of classes for multiclassification. See documentation for details.
        connectTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=60000)
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        customMetric : list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        depth : int
            Depth of the tree.Default value is 6.
        diffusionTemperature : float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        earlyStoppingRounds : int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        evalMetric : str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        featureBorderType : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        featureWeightsList : list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        featureWeightsMap : dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        featuresCol : str, default: "features"
            features column name
        firstFeatureUsePenaltiesList : list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        firstFeatureUsePenaltiesMap : dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        foldLenMultiplier : float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        foldPermutationBlock : int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        hasTime : bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        ignoredFeaturesIndices : list
            Feature indices to exclude from the training
        ignoredFeaturesNames : list
            Feature names to exclude from the training
        inputBorders : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        iterations : int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        l2LeafReg : float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        labelCol : str, default: "label"
            label column name
        leafEstimationBacktracking : ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        leafEstimationIterations : int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        leafEstimationMethod : ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        learningRate : float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        loggingLevel : ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        lossFunction : str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        metricPeriod : int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        modelShrinkMode : EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        modelShrinkRate : float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        mvsReg : float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        nanMode : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        odPval : float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        odType : EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        odWait : int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        oneHotMaxSize : int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        penaltiesCoefficient : float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        perFloatFeatureQuantizaton : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        perObjectFeaturePenaltiesList : list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        perObjectFeaturePenaltiesMap : dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        predictionCol : str, default: "prediction"
            prediction column name
        probabilityCol : str, default: "probability"
            Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        randomSeed : int
            The random seed used for training. Default value is 0.
        randomStrength : float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        rawPredictionCol : str, default: "rawPrediction"
            raw prediction (a.k.a. confidence) column name
        rsm : float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        samplingFrequency : ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        samplingUnit : ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        saveSnapshot : bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        scalePosWeight : float
            The weight for class 1 in binary classification. The value is used as a multiplier for the weights of objects from class 1. Default value is 1 (both classes have equal weight).
        scoreFunction : EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        snapshotFile : str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        snapshotInterval : datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        sparkPartitionCount : int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        subsample : float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        targetBorder : float
            If set, defines the border for converting target values to 0 and 1 classes.
        threadCount : int
            Number of CPU threads in parallel operations on client
        thresholds : list
            Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        trainDir : str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        useBestModel : bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        weightCol : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        workerInitializationTimeout : datetime.timedelta, default: datetime.timedelta(milliseconds=600000)
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        workerMaxFailures : int, default: 4
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)


    def getAllowConstLabel(self):
        """
        Returns
        -------
        bool
            Use it to train models with datasets that have equal label values for all objects.
        """
        return self.getOrDefault(self.allowConstLabel)

    def setAllowConstLabel(self, value):
        """
        Parameters
        ----------
        value : bool
            Use it to train models with datasets that have equal label values for all objects.
        """
        self._set(allowConstLabel=value)
        return self



    def getAllowWritingFiles(self):
        """
        Returns
        -------
        bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        """
        return self.getOrDefault(self.allowWritingFiles)

    def setAllowWritingFiles(self, value):
        """
        Parameters
        ----------
        value : bool
            Allow to write analytical and snapshot files during training. Enabled by default.
        """
        self._set(allowWritingFiles=value)
        return self



    def getApproxOnFullHistory(self):
        """
        Returns
        -------
        bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        """
        return self.getOrDefault(self.approxOnFullHistory)

    def setApproxOnFullHistory(self, value):
        """
        Parameters
        ----------
        value : bool
            Use all the preceding rows in the fold for calculating the approximated values. This mode is slower and in rare cases slightly more accurate.
        """
        self._set(approxOnFullHistory=value)
        return self



    def getAutoClassWeights(self):
        """
        Returns
        -------
        EAutoClassWeightsType
            Automatically calculate class weights based either on the total weight or the total number of objects in each class. The values are used as multipliers for the object weights. Default value is 'None'
        """
        return self.getOrDefault(self.autoClassWeights)

    def setAutoClassWeights(self, value):
        """
        Parameters
        ----------
        value : EAutoClassWeightsType
            Automatically calculate class weights based either on the total weight or the total number of objects in each class. The values are used as multipliers for the object weights. Default value is 'None'
        """
        self._set(autoClassWeights=value)
        return self



    def getBaggingTemperature(self):
        """
        Returns
        -------
        float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        """
        return self.getOrDefault(self.baggingTemperature)

    def setBaggingTemperature(self, value):
        """
        Parameters
        ----------
        value : float
            This parameter can be used if the selected bootstrap type is Bayesian. Possible values are in the range [0, +inf). The higher the value the more aggressive the bagging is.Default value in 1.0.
        """
        self._set(baggingTemperature=value)
        return self



    def getBestModelMinTrees(self):
        """
        Returns
        -------
        int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        """
        return self.getOrDefault(self.bestModelMinTrees)

    def setBestModelMinTrees(self, value):
        """
        Parameters
        ----------
        value : int
            The minimal number of trees that the best model should have. If set, the output model contains at least  the given number of trees even if the best model is located within these trees. Should be used with the useBestModel parameter. No limit by default.
        """
        self._set(bestModelMinTrees=value)
        return self



    def getBootstrapType(self):
        """
        Returns
        -------
        EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        """
        return self.getOrDefault(self.bootstrapType)

    def setBootstrapType(self, value):
        """
        Parameters
        ----------
        value : EBootstrapType
            Bootstrap type. Defines the method for sampling the weights of objects.The default value depends on the selected mode and processing unit type: QueryCrossEntropy, YetiRankPairwise, PairLogitPairwise: Bernoulli with the subsample parameter set to 0.5. MultiClass and MultiClassOneVsAll: Bayesian. Other modes: MVS with the subsample parameter set to 0.8.
        """
        self._set(bootstrapType=value)
        return self



    def getBorderCount(self):
        """
        Returns
        -------
        int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        """
        return self.getOrDefault(self.borderCount)

    def setBorderCount(self, value):
        """
        Parameters
        ----------
        value : int
            The number of splits for numerical features. Allowed values are integers from 1 to 65535 inclusively. Default value is 254.
        """
        self._set(borderCount=value)
        return self



    def getClassNames(self):
        """
        Returns
        -------
        list
            Allows to redefine the default values (consecutive integers).
        """
        return self.getOrDefault(self.classNames)

    def setClassNames(self, value):
        """
        Parameters
        ----------
        value : list
            Allows to redefine the default values (consecutive integers).
        """
        self._set(classNames=value)
        return self



    def getClassWeightsList(self):
        """
        Returns
        -------
        list
            List of weights for each class. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsMap.
        """
        return self.getOrDefault(self.classWeightsList)

    def setClassWeightsList(self, value):
        """
        Parameters
        ----------
        value : list
            List of weights for each class. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsMap.
        """
        self._set(classWeightsList=value)
        return self



    def getClassWeightsMap(self):
        """
        Returns
        -------
        dict
            Map from class name to weight. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsList.
        """
        return self.getOrDefault(self.classWeightsMap)

    def setClassWeightsMap(self, value):
        """
        Parameters
        ----------
        value : dict
            Map from class name to weight. The values are used as multipliers for the object weights.  This parameter is mutually exclusive with classWeightsList.
        """
        self._set(classWeightsMap=value)
        return self



    def getClassesCount(self):
        """
        Returns
        -------
        int
            The upper limit for the numeric class label. Defines the number of classes for multiclassification. See documentation for details.
        """
        return self.getOrDefault(self.classesCount)

    def setClassesCount(self, value):
        """
        Parameters
        ----------
        value : int
            The upper limit for the numeric class label. Defines the number of classes for multiclassification. See documentation for details.
        """
        self._set(classesCount=value)
        return self



    def getConnectTimeout(self):
        """
        Returns
        -------
        datetime.timedelta
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        """
        return self.getOrDefault(self.connectTimeout)

    def setConnectTimeout(self, value):
        """
        Parameters
        ----------
        value : datetime.timedelta
            Timeout to wait while establishing socket connections between TrainingDriver and workers.Default is 1 minute
        """
        self._set(connectTimeout=value)
        return self



    def getCustomMetric(self):
        """
        Returns
        -------
        list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        """
        return self.getOrDefault(self.customMetric)

    def setCustomMetric(self, value):
        """
        Parameters
        ----------
        value : list
            Metric values to output during training. These functions are not optimized and are displayed for  informational purposes only. Some metrics support optional parameters (see the Objectives and  metrics documentation section for details on each metric).
        """
        self._set(customMetric=value)
        return self



    def getDepth(self):
        """
        Returns
        -------
        int
            Depth of the tree.Default value is 6.
        """
        return self.getOrDefault(self.depth)

    def setDepth(self, value):
        """
        Parameters
        ----------
        value : int
            Depth of the tree.Default value is 6.
        """
        self._set(depth=value)
        return self



    def getDiffusionTemperature(self):
        """
        Returns
        -------
        float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        """
        return self.getOrDefault(self.diffusionTemperature)

    def setDiffusionTemperature(self, value):
        """
        Parameters
        ----------
        value : float
            The diffusion temperature of the Stochastic Gradient Langevin Boosting mode. Only non-negative values are supported. Default value is 10000.
        """
        self._set(diffusionTemperature=value)
        return self



    def getEarlyStoppingRounds(self):
        """
        Returns
        -------
        int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        """
        return self.getOrDefault(self.earlyStoppingRounds)

    def setEarlyStoppingRounds(self, value):
        """
        Parameters
        ----------
        value : int
            Sets the overfitting detector type to Iter and stops the training after the specified number of iterations since the iteration with the optimal metric value.
        """
        self._set(earlyStoppingRounds=value)
        return self



    def getEvalMetric(self):
        """
        Returns
        -------
        str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        return self.getOrDefault(self.evalMetric)

    def setEvalMetric(self, value):
        """
        Parameters
        ----------
        value : str
            The metric used for overfitting detection (if enabled) and best model selection (if enabled). Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        self._set(evalMetric=value)
        return self



    def getFeatureBorderType(self):
        """
        Returns
        -------
        EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        """
        return self.getOrDefault(self.featureBorderType)

    def setFeatureBorderType(self, value):
        """
        Parameters
        ----------
        value : EBorderSelectionType
            The quantization mode for numerical features. See documentation for details. Default value is 'GreedyLogSum'
        """
        self._set(featureBorderType=value)
        return self



    def getFeatureWeightsList(self):
        """
        Returns
        -------
        list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        """
        return self.getOrDefault(self.featureWeightsList)

    def setFeatureWeightsList(self, value):
        """
        Parameters
        ----------
        value : list
            Per-feature multiplication weights used when choosing the best split. Array indices correspond to feature indices. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsMap.
        """
        self._set(featureWeightsList=value)
        return self



    def getFeatureWeightsMap(self):
        """
        Returns
        -------
        dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        """
        return self.getOrDefault(self.featureWeightsMap)

    def setFeatureWeightsMap(self, value):
        """
        Parameters
        ----------
        value : dict
            Per-feature multiplication weights used when choosing the best split. Map is 'feature_name' -> weight. The score of each candidate is multiplied by the weights of features from the current split.This parameter is mutually exclusive with featureWeightsList.
        """
        self._set(featureWeightsMap=value)
        return self



    def getFeaturesCol(self):
        """
        Returns
        -------
        str
            features column name
        """
        return self.getOrDefault(self.featuresCol)

    def setFeaturesCol(self, value):
        """
        Parameters
        ----------
        value : str
            features column name
        """
        self._set(featuresCol=value)
        return self



    def getFirstFeatureUsePenaltiesList(self):
        """
        Returns
        -------
        list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        """
        return self.getOrDefault(self.firstFeatureUsePenaltiesList)

    def setFirstFeatureUsePenaltiesList(self, value):
        """
        Parameters
        ----------
        value : list
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesMap.
        """
        self._set(firstFeatureUsePenaltiesList=value)
        return self



    def getFirstFeatureUsePenaltiesMap(self):
        """
        Returns
        -------
        dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        """
        return self.getOrDefault(self.firstFeatureUsePenaltiesMap)

    def setFirstFeatureUsePenaltiesMap(self, value):
        """
        Parameters
        ----------
        value : dict
            Per-feature penalties for the first occurrence of the feature in the model. The given value is subtracted from the score if the current candidate is the first one to include the feature in the model. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with firstFeatureUsePenaltiesList.
        """
        self._set(firstFeatureUsePenaltiesMap=value)
        return self



    def getFoldLenMultiplier(self):
        """
        Returns
        -------
        float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        """
        return self.getOrDefault(self.foldLenMultiplier)

    def setFoldLenMultiplier(self, value):
        """
        Parameters
        ----------
        value : float
            Coefficient for changing the length of folds. The value must be greater than 1. The best validation result is achieved with minimum values. Default value is 2.0.
        """
        self._set(foldLenMultiplier=value)
        return self



    def getFoldPermutationBlock(self):
        """
        Returns
        -------
        int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        """
        return self.getOrDefault(self.foldPermutationBlock)

    def setFoldPermutationBlock(self, value):
        """
        Parameters
        ----------
        value : int
            Objects in the dataset are grouped in blocks before the random permutations. This parameter defines the size of the blocks. The smaller is the value, the slower is the training. Large values may result in quality degradation. Default value is 1.
        """
        self._set(foldPermutationBlock=value)
        return self



    def getHasTime(self):
        """
        Returns
        -------
        bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        """
        return self.getOrDefault(self.hasTime)

    def setHasTime(self, value):
        """
        Parameters
        ----------
        value : bool
            Use the order of objects in the input data (do not perform random permutations during Choosing the tree structure stage).
        """
        self._set(hasTime=value)
        return self



    def getIgnoredFeaturesIndices(self):
        """
        Returns
        -------
        list
            Feature indices to exclude from the training
        """
        return self.getOrDefault(self.ignoredFeaturesIndices)

    def setIgnoredFeaturesIndices(self, value):
        """
        Parameters
        ----------
        value : list
            Feature indices to exclude from the training
        """
        self._set(ignoredFeaturesIndices=value)
        return self



    def getIgnoredFeaturesNames(self):
        """
        Returns
        -------
        list
            Feature names to exclude from the training
        """
        return self.getOrDefault(self.ignoredFeaturesNames)

    def setIgnoredFeaturesNames(self, value):
        """
        Parameters
        ----------
        value : list
            Feature names to exclude from the training
        """
        self._set(ignoredFeaturesNames=value)
        return self



    def getInputBorders(self):
        """
        Returns
        -------
        str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        """
        return self.getOrDefault(self.inputBorders)

    def setInputBorders(self, value):
        """
        Parameters
        ----------
        value : str
            Load Custom quantization borders and missing value modes from a file (do not generate them)
        """
        self._set(inputBorders=value)
        return self



    def getIterations(self):
        """
        Returns
        -------
        int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        """
        return self.getOrDefault(self.iterations)

    def setIterations(self, value):
        """
        Parameters
        ----------
        value : int
            The maximum number of trees that can be built when solving machine learning problems. When using other parameters that limit the number of iterations, the final number of trees may be less than the number specified in this parameter. Default value is 1000.
        """
        self._set(iterations=value)
        return self



    def getL2LeafReg(self):
        """
        Returns
        -------
        float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        """
        return self.getOrDefault(self.l2LeafReg)

    def setL2LeafReg(self, value):
        """
        Parameters
        ----------
        value : float
            Coefficient at the L2 regularization term of the cost function. Any positive value is allowed. Default value is 3.0.
        """
        self._set(l2LeafReg=value)
        return self



    def getLabelCol(self):
        """
        Returns
        -------
        str
            label column name
        """
        return self.getOrDefault(self.labelCol)

    def setLabelCol(self, value):
        """
        Parameters
        ----------
        value : str
            label column name
        """
        self._set(labelCol=value)
        return self



    def getLeafEstimationBacktracking(self):
        """
        Returns
        -------
        ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        """
        return self.getOrDefault(self.leafEstimationBacktracking)

    def setLeafEstimationBacktracking(self, value):
        """
        Parameters
        ----------
        value : ELeavesEstimationStepBacktracking
            When the value of the leafEstimationIterations parameter is greater than 1, CatBoost makes several gradient or newton steps when calculating the resulting leaf values of a tree. The behaviour differs depending on the value of this parameter. See documentation for details. Default value is 'AnyImprovement'
        """
        self._set(leafEstimationBacktracking=value)
        return self



    def getLeafEstimationIterations(self):
        """
        Returns
        -------
        int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        """
        return self.getOrDefault(self.leafEstimationIterations)

    def setLeafEstimationIterations(self, value):
        """
        Parameters
        ----------
        value : int
            CatBoost might calculate leaf values using several gradient or newton steps instead of a single one. This parameter regulates how many steps are done in every tree when calculating leaf values.
        """
        self._set(leafEstimationIterations=value)
        return self



    def getLeafEstimationMethod(self):
        """
        Returns
        -------
        ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        """
        return self.getOrDefault(self.leafEstimationMethod)

    def setLeafEstimationMethod(self, value):
        """
        Parameters
        ----------
        value : ELeavesEstimation
            The method used to calculate the values in leaves. See documentation for details.
        """
        self._set(leafEstimationMethod=value)
        return self



    def getLearningRate(self):
        """
        Returns
        -------
        float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        """
        return self.getOrDefault(self.learningRate)

    def setLearningRate(self, value):
        """
        Parameters
        ----------
        value : float
            The learning rate. Used for reducing the gradient step. The default value is defined automatically for Logloss, MultiClass & RMSE loss functions depending on  the number of iterations if none of 'leaf_estimation_iterations', leaf_estimation_method', 'l2_leaf_reg' is set. In this case, the selected learning rate is printed to stdout and saved in the model. In other cases, the default value is 0.03.
        """
        self._set(learningRate=value)
        return self



    def getLoggingLevel(self):
        """
        Returns
        -------
        ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        """
        return self.getOrDefault(self.loggingLevel)

    def setLoggingLevel(self, value):
        """
        Parameters
        ----------
        value : ELoggingLevel
            The logging level to output to stdout. See documentation for details. Default value is 'Verbose'
        """
        self._set(loggingLevel=value)
        return self



    def getLossFunction(self):
        """
        Returns
        -------
        str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        return self.getOrDefault(self.lossFunction)

    def setLossFunction(self, value):
        """
        Parameters
        ----------
        value : str
            The metric to use in training. The specified value also determines the machine learning problem to solve. Some metrics support optional parameters (see the Objectives and metrics documentation section for details on each metric).
        """
        self._set(lossFunction=value)
        return self



    def getMetricPeriod(self):
        """
        Returns
        -------
        int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        """
        return self.getOrDefault(self.metricPeriod)

    def setMetricPeriod(self, value):
        """
        Parameters
        ----------
        value : int
            The frequency of iterations to calculate the values of objectives and metrics. The value should be a  positive integer. The usage of this parameter speeds up the training. Default value is 1.
        """
        self._set(metricPeriod=value)
        return self



    def getModelShrinkMode(self):
        """
        Returns
        -------
        EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        """
        return self.getOrDefault(self.modelShrinkMode)

    def setModelShrinkMode(self, value):
        """
        Parameters
        ----------
        value : EModelShrinkMode
            Determines how the actual model shrinkage coefficient is calculated at each iteration. See documentation for details. Default value is 'Constant'
        """
        self._set(modelShrinkMode=value)
        return self



    def getModelShrinkRate(self):
        """
        Returns
        -------
        float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        """
        return self.getOrDefault(self.modelShrinkRate)

    def setModelShrinkRate(self, value):
        """
        Parameters
        ----------
        value : float
            The constant used to calculate the coefficient for multiplying the model on each iteration. See documentation for details.
        """
        self._set(modelShrinkRate=value)
        return self



    def getMvsReg(self):
        """
        Returns
        -------
        float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        """
        return self.getOrDefault(self.mvsReg)

    def setMvsReg(self, value):
        """
        Parameters
        ----------
        value : float
            Affects the weight of the denominator and can be used for balancing between the importance and Bernoulli sampling (setting it to 0 implies importance sampling and to +Inf - Bernoulli).Note: This parameter is supported only for the MVS sampling method.
        """
        self._set(mvsReg=value)
        return self



    def getNanMode(self):
        """
        Returns
        -------
        ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        """
        return self.getOrDefault(self.nanMode)

    def setNanMode(self, value):
        """
        Parameters
        ----------
        value : ENanMode
            The method for processing missing values in the input dataset. See documentation for details. Default value is 'Min'
        """
        self._set(nanMode=value)
        return self



    def getOdPval(self):
        """
        Returns
        -------
        float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        """
        return self.getOrDefault(self.odPval)

    def setOdPval(self, value):
        """
        Parameters
        ----------
        value : float
            The threshold for the IncToDec overfitting detector type. The training is stopped when the specified value is reached. Requires that a validation dataset was input. See documentation for details.Turned off by default.
        """
        self._set(odPval=value)
        return self



    def getOdType(self):
        """
        Returns
        -------
        EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        """
        return self.getOrDefault(self.odType)

    def setOdType(self, value):
        """
        Parameters
        ----------
        value : EOverfittingDetectorType
            The type of the overfitting detector to use. See documentation for details. Default value is 'IncToDec'
        """
        self._set(odType=value)
        return self



    def getOdWait(self):
        """
        Returns
        -------
        int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        """
        return self.getOrDefault(self.odWait)

    def setOdWait(self, value):
        """
        Parameters
        ----------
        value : int
            The number of iterations to continue the training after the iteration with the optimal metric value. See documentation for details. Default value is 20.
        """
        self._set(odWait=value)
        return self



    def getOneHotMaxSize(self):
        """
        Returns
        -------
        int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        """
        return self.getOrDefault(self.oneHotMaxSize)

    def setOneHotMaxSize(self, value):
        """
        Parameters
        ----------
        value : int
            Use one-hot encoding for all categorical features with a number of different values less than or equal to the given parameter value. Ctrs are not calculated for such features.
        """
        self._set(oneHotMaxSize=value)
        return self



    def getPenaltiesCoefficient(self):
        """
        Returns
        -------
        float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        """
        return self.getOrDefault(self.penaltiesCoefficient)

    def setPenaltiesCoefficient(self, value):
        """
        Parameters
        ----------
        value : float
            A single-value common coefficient to multiply all penalties. Non-negative values are supported. Default value is 1.0.
        """
        self._set(penaltiesCoefficient=value)
        return self



    def getPerFloatFeatureQuantizaton(self):
        """
        Returns
        -------
        list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        """
        return self.getOrDefault(self.perFloatFeatureQuantizaton)

    def setPerFloatFeatureQuantizaton(self, value):
        """
        Parameters
        ----------
        value : list
            The quantization description for the given list of features (one or more).Description format for a single feature: FeatureId[:border_count=BorderCount][:nan_mode=BorderType][:border_type=border_selection_method]
        """
        self._set(perFloatFeatureQuantizaton=value)
        return self



    def getPerObjectFeaturePenaltiesList(self):
        """
        Returns
        -------
        list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        """
        return self.getOrDefault(self.perObjectFeaturePenaltiesList)

    def setPerObjectFeaturePenaltiesList(self, value):
        """
        Parameters
        ----------
        value : list
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Array indices correspond to feature indices. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesMap.
        """
        self._set(perObjectFeaturePenaltiesList=value)
        return self



    def getPerObjectFeaturePenaltiesMap(self):
        """
        Returns
        -------
        dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        """
        return self.getOrDefault(self.perObjectFeaturePenaltiesMap)

    def setPerObjectFeaturePenaltiesMap(self, value):
        """
        Parameters
        ----------
        value : dict
            Per-object penalties for the first use of the feature for the object. The given value is multiplied by the number of objects that are divided by the current split and use the feature for the first time. Map is 'feature_name' -> penalty. See documentation for details. This parameter is mutually exclusive with perObjectFeaturePenaltiesList.
        """
        self._set(perObjectFeaturePenaltiesMap=value)
        return self



    def getPredictionCol(self):
        """
        Returns
        -------
        str
            prediction column name
        """
        return self.getOrDefault(self.predictionCol)

    def setPredictionCol(self, value):
        """
        Parameters
        ----------
        value : str
            prediction column name
        """
        self._set(predictionCol=value)
        return self



    def getProbabilityCol(self):
        """
        Returns
        -------
        str
            Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        """
        return self.getOrDefault(self.probabilityCol)

    def setProbabilityCol(self, value):
        """
        Parameters
        ----------
        value : str
            Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        """
        self._set(probabilityCol=value)
        return self



    def getRandomSeed(self):
        """
        Returns
        -------
        int
            The random seed used for training. Default value is 0.
        """
        return self.getOrDefault(self.randomSeed)

    def setRandomSeed(self, value):
        """
        Parameters
        ----------
        value : int
            The random seed used for training. Default value is 0.
        """
        self._set(randomSeed=value)
        return self



    def getRandomStrength(self):
        """
        Returns
        -------
        float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        """
        return self.getOrDefault(self.randomStrength)

    def setRandomStrength(self, value):
        """
        Parameters
        ----------
        value : float
            The amount of randomness to use for scoring splits when the tree structure is selected. Use this parameter to avoid overfitting the model. See documentation for details. Default value is 1.0
        """
        self._set(randomStrength=value)
        return self



    def getRawPredictionCol(self):
        """
        Returns
        -------
        str
            raw prediction (a.k.a. confidence) column name
        """
        return self.getOrDefault(self.rawPredictionCol)

    def setRawPredictionCol(self, value):
        """
        Parameters
        ----------
        value : str
            raw prediction (a.k.a. confidence) column name
        """
        self._set(rawPredictionCol=value)
        return self



    def getRsm(self):
        """
        Returns
        -------
        float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        """
        return self.getOrDefault(self.rsm)

    def setRsm(self, value):
        """
        Parameters
        ----------
        value : float
            Random subspace method. The percentage of features to use at each split selection, when features are selected over again at random. The value must be in the range (0;1]. Default value is 1.
        """
        self._set(rsm=value)
        return self



    def getSamplingFrequency(self):
        """
        Returns
        -------
        ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        """
        return self.getOrDefault(self.samplingFrequency)

    def setSamplingFrequency(self, value):
        """
        Parameters
        ----------
        value : ESamplingFrequency
            Frequency to sample weights and objects when building trees. Default value is 'PerTreeLevel'
        """
        self._set(samplingFrequency=value)
        return self



    def getSamplingUnit(self):
        """
        Returns
        -------
        ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        """
        return self.getOrDefault(self.samplingUnit)

    def setSamplingUnit(self, value):
        """
        Parameters
        ----------
        value : ESamplingUnit
            The sampling scheme, see documentation for details. Default value is 'Object'
        """
        self._set(samplingUnit=value)
        return self



    def getSaveSnapshot(self):
        """
        Returns
        -------
        bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        """
        return self.getOrDefault(self.saveSnapshot)

    def setSaveSnapshot(self, value):
        """
        Parameters
        ----------
        value : bool
            Enable snapshotting for restoring the training progress after an interruption. If enabled, the default  period for making snapshots is 600 seconds. Use the snapshotInterval parameter to change this period.
        """
        self._set(saveSnapshot=value)
        return self



    def getScalePosWeight(self):
        """
        Returns
        -------
        float
            The weight for class 1 in binary classification. The value is used as a multiplier for the weights of objects from class 1. Default value is 1 (both classes have equal weight).
        """
        return self.getOrDefault(self.scalePosWeight)

    def setScalePosWeight(self, value):
        """
        Parameters
        ----------
        value : float
            The weight for class 1 in binary classification. The value is used as a multiplier for the weights of objects from class 1. Default value is 1 (both classes have equal weight).
        """
        self._set(scalePosWeight=value)
        return self



    def getScoreFunction(self):
        """
        Returns
        -------
        EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        """
        return self.getOrDefault(self.scoreFunction)

    def setScoreFunction(self, value):
        """
        Parameters
        ----------
        value : EScoreFunction
            The score type used to select the next split during the tree construction. See documentation for details. Default value is 'Cosine'
        """
        self._set(scoreFunction=value)
        return self



    def getSnapshotFile(self):
        """
        Returns
        -------
        str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        """
        return self.getOrDefault(self.snapshotFile)

    def setSnapshotFile(self, value):
        """
        Parameters
        ----------
        value : str
            The name of the file to save the training progress information in. This file is used for recovering training after an interruption.
        """
        self._set(snapshotFile=value)
        return self



    def getSnapshotInterval(self):
        """
        Returns
        -------
        datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        """
        return self.getOrDefault(self.snapshotInterval)

    def setSnapshotInterval(self, value):
        """
        Parameters
        ----------
        value : datetime.timedelta
            The interval between saving snapshots. See documentation for details. Default value is 600 seconds.
        """
        self._set(snapshotInterval=value)
        return self



    def getSparkPartitionCount(self):
        """
        Returns
        -------
        int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        """
        return self.getOrDefault(self.sparkPartitionCount)

    def setSparkPartitionCount(self, value):
        """
        Parameters
        ----------
        value : int
            The number of partitions used during training. Corresponds to the number of active parallel tasks. Set to the number of active executors by default
        """
        self._set(sparkPartitionCount=value)
        return self



    def getSubsample(self):
        """
        Returns
        -------
        float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        """
        return self.getOrDefault(self.subsample)

    def setSubsample(self, value):
        """
        Parameters
        ----------
        value : float
            Sample rate for bagging. The default value depends on the dataset size and the bootstrap type, see documentation for details.
        """
        self._set(subsample=value)
        return self



    def getTargetBorder(self):
        """
        Returns
        -------
        float
            If set, defines the border for converting target values to 0 and 1 classes.
        """
        return self.getOrDefault(self.targetBorder)

    def setTargetBorder(self, value):
        """
        Parameters
        ----------
        value : float
            If set, defines the border for converting target values to 0 and 1 classes.
        """
        self._set(targetBorder=value)
        return self



    def getThreadCount(self):
        """
        Returns
        -------
        int
            Number of CPU threads in parallel operations on client
        """
        return self.getOrDefault(self.threadCount)

    def setThreadCount(self, value):
        """
        Parameters
        ----------
        value : int
            Number of CPU threads in parallel operations on client
        """
        self._set(threadCount=value)
        return self



    def getThresholds(self):
        """
        Returns
        -------
        list
            Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        """
        return self.getOrDefault(self.thresholds)

    def setThresholds(self, value):
        """
        Parameters
        ----------
        value : list
            Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        """
        self._set(thresholds=value)
        return self



    def getTrainDir(self):
        """
        Returns
        -------
        str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        """
        return self.getOrDefault(self.trainDir)

    def setTrainDir(self, value):
        """
        Parameters
        ----------
        value : str
            The directory for storing the files on Driver node generated during training. Default value is 'catboost_info'
        """
        self._set(trainDir=value)
        return self



    def getUseBestModel(self):
        """
        Returns
        -------
        bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        """
        return self.getOrDefault(self.useBestModel)

    def setUseBestModel(self, value):
        """
        Parameters
        ----------
        value : bool
            If this parameter is set, the number of trees that are saved in the resulting model is selected based on the optimal value of the evalMetric. This option requires a validation dataset to be provided.
        """
        self._set(useBestModel=value)
        return self



    def getWeightCol(self):
        """
        Returns
        -------
        str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        """
        return self.getOrDefault(self.weightCol)

    def setWeightCol(self, value):
        """
        Parameters
        ----------
        value : str
            weight column name. If this is not set or empty, we treat all instance weights as 1.0
        """
        self._set(weightCol=value)
        return self



    def getWorkerInitializationTimeout(self):
        """
        Returns
        -------
        datetime.timedelta
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        """
        return self.getOrDefault(self.workerInitializationTimeout)

    def setWorkerInitializationTimeout(self, value):
        """
        Parameters
        ----------
        value : datetime.timedelta
            Timeout to wait until CatBoost workers on Spark executors are initalized and sent their info to master. Depends on dataset size. Default is 10 minutes
        """
        self._set(workerInitializationTimeout=value)
        return self



    def getWorkerMaxFailures(self):
        """
        Returns
        -------
        int
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
        """
        return self.getOrDefault(self.workerMaxFailures)

    def setWorkerMaxFailures(self, value):
        """
        Parameters
        ----------
        value : int
            Number of individual CatBoost workers failures before giving up training. Should be greater than or equal to 1. Default is 4
        """
        self._set(workerMaxFailures=value)
        return self




    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return CatBoostMLReader(cls)

    def _create_model(self, java_model):
        return CatBoostClassificationModel(java_model)

    def _fit_with_eval(self, trainDatasetAsJavaObject, evalDatasetsAsJavaObject, params=None):
        """
        Implementation of fit with eval datasets with no more than one set of optional parameters
        """
        if params:
            return self.copy(params)._fit_with_eval(trainDatasetAsJavaObject, evalDatasetsAsJavaObject)
        else:
            self._transfer_params_to_java()
            java_model = self._java_obj.fit(trainDatasetAsJavaObject, evalDatasetsAsJavaObject)
            return CatBoostClassificationModel(java_model)

    def fit(self, dataset, params=None, evalDatasets=None):
        """
        Extended variant of standard Estimator's fit method
        that accepts CatBoost's Pool s and allows to specify additional
        datasets for computing evaluation metrics and overfitting detection similarily to CatBoost's other APIs.
        
        Parameters
        ---------- 
        dataset : Pool or DataFrame
          The input training dataset.
        params : dict or list or tuple, optional
          an optional param map that overrides embedded params. If a list/tuple of
          param maps is given, this calls fit on each param map and returns a list of
          models.
        evalDatasets : Pools, optional
          The validation datasets used for the following processes:
           - overfitting detector
           - best iteration selection
           - monitoring metrics' changes
        
        Returns
        -------
        trained model(s): CatBoostClassificationModel or a list of trained CatBoostClassificationModel
        """
        if (isinstance(dataset, DataFrame)):
            if evalDatasets is not None:
                raise RuntimeError("if dataset has type DataFrame no evalDatasets are supported")
            return JavaEstimator.fit(self, dataset, params)
        else:
            sc = SparkContext._active_spark_context

            trainDatasetAsJavaObject = _py2java(sc, dataset)
            evalDatasetCount = 0 if (evalDatasets is None) else len(evalDatasets)

            # need to create it because default mapping for python list is ArrayList, not Array
            evalDatasetsAsJavaObject = sc._gateway.new_array(sc._jvm.ai.catboost.spark.Pool, evalDatasetCount)
            for i in range(evalDatasetCount):
                evalDatasetsAsJavaObject[i] = _py2java(sc, evalDatasets[i])

            def _fit_with_eval(params):
                return self._fit_with_eval(trainDatasetAsJavaObject, evalDatasetsAsJavaObject, params)

            if (params is None) or isinstance(params, dict):
                return _fit_with_eval(params)
            if isinstance(params, (list, tuple)):
                models = []
                for paramsInstance in params:
                    models.append(_fit_with_eval(paramsInstance))
                return models
            else:
                raise TypeError("Params must be either a param map or a list/tuple of param maps, "
                                "but got %s." % type(params))

@inherit_doc
class CatBoostClassificationModel(_JavaProbabilisticClassificationModel, MLReadable, JavaMLWritable):
    """
    Classification model trained by CatBoost. Use CatBoostClassifier to train it
    """
    def __init__(self, java_model=None):
        super(CatBoostClassificationModel, self).__init__(java_model)
        self.featuresCol = Param(self, "featuresCol", "features column name")
        self._setDefault(featuresCol="features")
        self.labelCol = Param(self, "labelCol", "label column name")
        self._setDefault(labelCol="label")
        self.predictionCol = Param(self, "predictionCol", "prediction column name")
        self._setDefault(predictionCol="prediction")
        self.probabilityCol = Param(self, "probabilityCol", "Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities")
        self._setDefault(probabilityCol="probability")
        self.rawPredictionCol = Param(self, "rawPredictionCol", "raw prediction (a.k.a. confidence) column name")
        self._setDefault(rawPredictionCol="rawPrediction")
        self.thresholds = Param(self, "thresholds", "Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold")
        if java_model is not None:
            self._transfer_params_from_java()


    @keyword_only
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", probabilityCol="probability", rawPredictionCol="rawPrediction", thresholds=None):
        """
        Set the (keyword only) parameters

        Parameters
        ----------
        featuresCol : str, default: "features"
            features column name
        labelCol : str, default: "label"
            label column name
        predictionCol : str, default: "prediction"
            prediction column name
        probabilityCol : str, default: "probability"
            Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        rawPredictionCol : str, default: "rawPrediction"
            raw prediction (a.k.a. confidence) column name
        thresholds : list
            Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)


    def getFeaturesCol(self):
        """
        Returns
        -------
        str
            features column name
        """
        return self.getOrDefault(self.featuresCol)

    def setFeaturesCol(self, value):
        """
        Parameters
        ----------
        value : str
            features column name
        """
        self._set(featuresCol=value)
        return self



    def getLabelCol(self):
        """
        Returns
        -------
        str
            label column name
        """
        return self.getOrDefault(self.labelCol)

    def setLabelCol(self, value):
        """
        Parameters
        ----------
        value : str
            label column name
        """
        self._set(labelCol=value)
        return self



    def getPredictionCol(self):
        """
        Returns
        -------
        str
            prediction column name
        """
        return self.getOrDefault(self.predictionCol)

    def setPredictionCol(self, value):
        """
        Parameters
        ----------
        value : str
            prediction column name
        """
        self._set(predictionCol=value)
        return self



    def getProbabilityCol(self):
        """
        Returns
        -------
        str
            Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        """
        return self.getOrDefault(self.probabilityCol)

    def setProbabilityCol(self, value):
        """
        Parameters
        ----------
        value : str
            Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        """
        self._set(probabilityCol=value)
        return self



    def getRawPredictionCol(self):
        """
        Returns
        -------
        str
            raw prediction (a.k.a. confidence) column name
        """
        return self.getOrDefault(self.rawPredictionCol)

    def setRawPredictionCol(self, value):
        """
        Parameters
        ----------
        value : str
            raw prediction (a.k.a. confidence) column name
        """
        self._set(rawPredictionCol=value)
        return self



    def getThresholds(self):
        """
        Returns
        -------
        list
            Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        """
        return self.getOrDefault(self.thresholds)

    def setThresholds(self, value):
        """
        Parameters
        ----------
        value : list
            Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        """
        self._set(thresholds=value)
        return self




    @staticmethod
    def _from_java(java_model):
        return CatBoostClassificationModel(java_model)

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return CatBoostMLReader(cls)

    def saveNativeModel(self, fileName, format=EModelType.CatboostBinary, exportParameters=None, pool=None):
        """
        Save the model to a local file.
        See https://catboost.ai/docs/concepts/python-reference_catboostclassifier_save_model.html
        for detailed parameters description
        """
        return self._call_java("saveNativeModel", fileName, format, exportParameters, pool)

    @staticmethod
    def loadNativeModel(fileName, format=EModelType.CatboostBinary):
        """
        Load the model from a local file.
        See https://catboost.ai/docs/concepts/python-reference_catboostclassifier_load_model.html
        for detailed parameters description
        """
        sc = SparkContext._active_spark_context
        java_model = sc._jvm.ai.catboost.spark.CatBoostClassificationModel.loadNativeModel(fileName, _py2java(sc, format))
        return CatBoostClassificationModel(java_model)


    def transformPool(self, pool):
        """
        This function is useful when the dataset has been already quantized but works with any Pool
        """
        return self._call_java("transformPool", pool)


    def getFeatureImportance(self, 
                             fstrType=EFstrType.FeatureImportance,
                             data=None,
                             calcType=ECalcTypeShapValues.Regular
                            ):
        """
        Parameters
        ----------
        fstrType : EFstrType
            Supported values are FeatureImportance, PredictionValuesChange, LossFunctionChange, PredictionDiff
        data : Pool
            if fstrType is PredictionDiff it is required and must contain 2 samples
            if fstrType is PredictionValuesChange this param is required in case if model was explicitly trained
            with flag to store no leaf weights.
            otherwise it can be null
        calcType : ECalcTypeShapValues
            Used only for PredictionValuesChange. 
            Possible values:
              - Regular
                 Calculate regular SHAP values
              - Approximate
                 Calculate approximate SHAP values
              - Exact
                 Calculate exact SHAP values

        Returns
        -------
        list of float
            array of feature importances (index corresponds to the order of features in the model)
        """
        return self._call_java("getFeatureImportance", fstrType, data, calcType)

    def getFeatureImportancePrettified(self, 
                                       fstrType=EFstrType.FeatureImportance,
                                       data=None,
                                       calcType=ECalcTypeShapValues.Regular
                                      ):
        """
        Parameters
        ----------
        fstrType : EFstrType
            Supported values are FeatureImportance, PredictionValuesChange, LossFunctionChange, PredictionDiff
        data : Pool
            if fstrType is PredictionDiff it is required and must contain 2 samples
            if fstrType is PredictionValuesChange this param is required in case if model was explicitly trained
            with flag to store no leaf weights.
            otherwise it can be null
        calcType : ECalcTypeShapValues
            Used only for PredictionValuesChange. 
            Possible values:

              - Regular
                 Calculate regular SHAP values
              - Approximate
                 Calculate approximate SHAP values
              - Exact
                 Calculate exact SHAP values

        Returns
        -------
        list of FeatureImportance
            array of feature importances sorted in descending order by importance
        """
        return self._call_java("getFeatureImportancePrettified", fstrType, data, calcType)

    def getFeatureImportanceShapValues(self,
                                       data,
                                       preCalcMode=EPreCalcShapValues.Auto,
                                       calcType=ECalcTypeShapValues.Regular,
                                       modelOutputType=EExplainableModelOutput.Raw,
                                       referenceData=None,
                                       outputColumns=None
                                      ):
        """
        Parameters
        ----------
        data : Pool
            dataset to calculate SHAP values for
        preCalcMode : EPreCalcShapValues
            Possible values:
               - Auto
                  Use direct SHAP Values calculation only if data size is smaller than average leaves number
                  (the best of two strategies below is chosen).
               - UsePreCalc
                  Calculate SHAP Values for every leaf in preprocessing. Final complexity is
                  O(NT(D+F))+O(TL^2 D^2) where N is the number of documents(objects), T - number of trees,
                  D - average tree depth, F - average number of features in tree, L - average number of leaves in tree
                  This is much faster (because of a smaller constant) than direct calculation when N >> L
               - NoPreCalc
                  Use direct SHAP Values calculation calculation with complexity O(NTLD^2). Direct algorithm
                  is faster when N < L (algorithm from https://arxiv.org/abs/1802.03888)
        calcType : ECalcTypeShapValues
            Possible values:

              - Regular
                 Calculate regular SHAP values
              - Approximate
                 Calculate approximate SHAP values
              - Exact
                 Calculate exact SHAP values
        referenceData : Pool
            reference data for Independent Tree SHAP values from https://arxiv.org/abs/1905.04610v1
            if referenceData is not null, then Independent Tree SHAP values are calculated
        outputColumns : list of str
            columns from data to add to output DataFrame, if None - add all columns

        Returns
        -------
        DataFrame
            - for regression and binclass models: 
              contains outputColumns and "shapValues" column with Vector of length (n_features + 1) with SHAP values
            - for multiclass models:
              contains outputColumns and "shapValues" column with Matrix of shape (n_classes x (n_features + 1)) with SHAP values
        """
        return self._call_java(
            "getFeatureImportanceShapValues", 
            data, 
            preCalcMode,
            calcType,
            modelOutputType,
            referenceData,
            outputColumns
        )

    def getFeatureImportanceShapInteractionValues(self,
                                                  data,
                                                  featureIndices=None,
                                                  featureNames=None,
                                                  preCalcMode=EPreCalcShapValues.Auto,
                                                  calcType=ECalcTypeShapValues.Regular,
                                                  outputColumns=None):
        """
        SHAP interaction values are calculated for all features pairs if nor featureIndices nor featureNames 
          are specified.

        Parameters
        ----------
        data : Pool
            dataset to calculate SHAP interaction values
        featureIndices : (int, int), optional
            pair of features indices to calculate SHAP interaction values for.
        featureNames : (str, str), optional
            pair of features names to calculate SHAP interaction values for.
        preCalcMode : EPreCalcShapValues
            Possible values:

            - Auto
                Use direct SHAP Values calculation only if data size is smaller than average leaves number
                (the best of two strategies below is chosen).
            - UsePreCalc
                Calculate SHAP Values for every leaf in preprocessing. Final complexity is
                O(NT(D+F))+O(TL^2 D^2) where N is the number of documents(objects), T - number of trees,
                D - average tree depth, F - average number of features in tree, L - average number of leaves in tree
                This is much faster (because of a smaller constant) than direct calculation when N >> L
            - NoPreCalc
                Use direct SHAP Values calculation calculation with complexity O(NTLD^2). Direct algorithm
                is faster when N < L (algorithm from https://arxiv.org/abs/1802.03888)
        calcType : ECalcTypeShapValues
            Possible values:

              - Regular
                  Calculate regular SHAP values
              - Approximate
                  Calculate approximate SHAP values
              - Exact
                  Calculate exact SHAP values
        outputColumns : list of str
            columns from data to add to output DataFrame, if None - add all columns

        Returns
        -------
        DataFrame
            - for regression and binclass models: 
              contains outputColumns and "featureIdx1", "featureIdx2", "shapInteractionValue" columns
            - for multiclass models:
              contains outputColumns and "classIdx", "featureIdx1", "featureIdx2", "shapInteractionValue" columns
        """
        return self._call_java(
            "getFeatureImportanceShapInteractionValues", 
            data,
            featureIndices,
            featureNames,
            preCalcMode, 
            calcType,
            outputColumns
        )

    def getFeatureImportanceInteraction(self):
        """
        Returns
        -------
        list of FeatureInteractionScore
        """
        return self._call_java("getFeatureImportanceInteraction")


