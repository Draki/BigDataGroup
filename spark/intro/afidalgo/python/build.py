import os
import sys
from pybuilder.core import init, use_plugin

use_plugin("python.core")
use_plugin("python.install_dependencies")
use_plugin("python.unittest")


default_task = ["install_dependencies", "publish"]

@init
def initialize(project):	
    os.environ['SPARK_HOME']="/Users/alvarofidalgo/spark-1.5.1-bin-hadoop2.6"
    sys.path.append("/Users/alvarofidalgo/spark-1.5.1-bin-hadoop2.6/python/")
