export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"

export PYSPARK_SUBMIT_ARGS="--master spark://a0a24058f00e:7077"

export JAVA_HOME=/usr/local/Cellar/openjdk@11/11.0.15

export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH"
