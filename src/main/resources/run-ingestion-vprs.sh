#!/bin/bash

#fonte="$1"
fonte="vprs"

export SPARK_MAJOR_VERSION=2

spark-submit vprs-assembly-1.0.jar --fonte $fonte
   
