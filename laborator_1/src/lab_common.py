"""Configurație comună pentru notebook, producer, streaming și dashboard."""
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"                  # DOAR review.json (Etapa 1 citește tot directorul)
RAW_DIMS_DIR = DATA_DIR / "raw_dims"        # business.json, user.json
DELTA_DIR = DATA_DIR / "delta"
PROCESSED_DIR = DATA_DIR / "processed_delta"
DWH_DIR = DATA_DIR / "dwh"
STREAM_SAMPLE_DIR = DATA_DIR / "stream_sample"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
FIG_DIR = PROJECT_ROOT / "docs" / "figures"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "yelp_reviews")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "yelp_stream")
MONGO_COLL = os.getenv("MONGO_COLL", "window_agg")


def setup_env():
    """Java din env-ul micromamba, Python pentru worker-i, fus orar UTC."""
    os.environ["TZ"] = "UTC"
    time.tzset()
    prefix = Path(sys.prefix)
    for c in [Path(os.environ.get("JAVA_HOME", "/nonexistent")), prefix / "lib" / "jvm", prefix]:
        if (c / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(c)
            os.environ["PATH"] = str(c / "bin") + os.pathsep + os.environ["PATH"]
            break
    else:
        raise FileNotFoundError("Java nu a fost găsit în env.")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ.pop("SPARK_HOME", None)
    for d in [PROCESSED_DIR, DWH_DIR, STREAM_SAMPLE_DIR, CHECKPOINT_DIR, FIG_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def get_spark(app="lab1", master="local[*]", driver_memory="8g", kafka=True):
    """SparkSession cu Delta (+ Kafka). Pachetele NU se pot schimba după pornire."""
    setup_env()
    import pyspark
    import delta
    from importlib.metadata import version
    from pyspark.sql import SparkSession

    assert hasattr(delta, "configure_spark_with_delta_pip"), "Rulați: pip uninstall -y delta"
    spark_mm = ".".join(pyspark.__version__.split(".")[:2])
    packages = [f"io.delta:delta-spark_{spark_mm}_2.13:{version('delta-spark')}"]
    if kafka:
        packages.append(f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}")

    tz_opt = "-Duser.timezone=UTC"
    spark = (
        SparkSession.builder.master(master).appName(app)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", tz_opt)
        .config("spark.executor.extraJavaOptions", tz_opt)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
