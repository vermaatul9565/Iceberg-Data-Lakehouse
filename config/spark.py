import pyspark
from pyspark.sql import SparkSession
import os

class SparkIcebergConfig:
    """Centralized Spark configuration for Iceberg"""
    
    @staticmethod
    def get_common_config():
        """Get common Spark configurations"""
        return {
            "spark.ui.port": "4040",
            "spark.ui.enabled": "true",
            "spark.ui.showConsoleProgress": "false",  # Reduce console output
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.debug.maxToStringFields": "100000",
            # Suppress metrics warnings
            "spark.metrics.conf.*.sink.file.class": "org.apache.hadoop.metrics2.sink.FileSink",
            "spark.metrics.conf.*.sink.file.filename": "/dev/null",
        }
    
    @staticmethod
    def get_glue_config():
        config = SparkIcebergConfig.get_common_config()
        config.update({
            "spark.jars.packages": (
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.8.1,"
                "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
            ),
            "spark.jars.excludes": "org.wildfly.openssl:wildfly-openssl",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.glue": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.glue.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.glue.warehouse": "s3a://9565atulverma-s3-bucket/data-lakehouse/",
            "spark.sql.catalog.glue.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.defaultCatalog": "glue",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.s3a.impl": "org.apache.hadoop.fs.s3a.S3A",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-south-1.amazonaws.com",
            "spark.hadoop.fs.s3a.endpoint.region": "ap-south-1",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.block.size": "134217728",
            "spark.hadoop.fs.s3a.multipart.size": "134217728",
            "spark.hadoop.fs.s3a.multipart.threshold": "134217728"
        })
        return config
    
    @staticmethod
    def get_local_config():
        """Get configuration for local Hadoop catalog"""
        config = SparkIcebergConfig.get_common_config()
        config.update({
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.8.1",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": "/home/iceberg/warehouse",
            "spark.sql.defaultCatalog": "local"
        })
        return config
    
    @staticmethod
    def get_rest_config():
        """Get configuration for REST catalog"""
        config = SparkIcebergConfig.get_common_config()
        config.update({
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.8.1",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.rest": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.rest.type": "rest",
            "spark.sql.catalog.rest.uri": "http://rest:8181",
            "spark.sql.defaultCatalog": "rest"
        })
        return config
    
    @staticmethod
    def create_session(config_type="local", app_name="IcebergApp"):
        """Create Spark session with specified configuration"""
        if config_type == "glue":
            config = SparkIcebergConfig.get_glue_config()
        elif config_type == "rest":
            config = SparkIcebergConfig.get_rest_config()
        else:
            config = SparkIcebergConfig.get_local_config()
        
        conf = pyspark.SparkConf()
        for key, value in config.items():
            conf.set(key, value)

        # print("Spark Configuration:",conf.getAll())
        
        return SparkSession.builder \
            .appName(app_name) \
            .config(conf=conf) \
            .getOrCreate()

# # Usage in your notebook:
# from spark_config import SparkIcebergConfig

# # Stop existing session
# if 'spark' in globals():
#     spark.stop()

# # Create new session with desired configuration
# spark = SparkIcebergConfig.create_session("local")  # or "glue" or "rest"