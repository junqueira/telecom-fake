#!/bin/bash

# Recupera as datas
fonte="$1"
input_start="$2"
input_end="$3"

# Verifica se a data é válida
startdate=$(TZ=BRST date -I -d "$input_start") || exit -1
enddate=$(TZ=BRST date -I -d "$input_end")     || exit -1

# Informa que deve utilizar o spark 2
export SPARK_MAJOR_VERSION=2

# Loop das datas
d="$startdate"
while [ "$d" != "$enddate" ]; do

    # Formata a data no estilo que o banco de dados aceita
    day=$(TZ=BRST date -d "$d" +%Y%m%d)

    export SPARK_MAJOR_VERSION=2;
    spark-submit vrps-assembly-1.0.jar --fonte $fonte --dtfoto $day
    
    d=$(TZ=BRST date -I -d "$d + 1 day")
done
