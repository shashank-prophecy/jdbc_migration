from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from test.config.ConfigStore import *
from test.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("test")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/test")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/test", config = Config)(pipeline)

if __name__ == "__main__":
    main()
