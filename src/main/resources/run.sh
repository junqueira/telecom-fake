#!/bin/bash

fonte="$1"
base="$2"
file_hash="$3"
file_ref="$4"
file_dest="$5"
input_start="$2"
input_end="$3"
repalertas="$4"

limDm="~"
dmenos=1

if [[ -z $fonte ]]; then
    echo "Favor informar a fonte."
    exit 134
fi

#if [[ "$fonte" =~ $limDm ]]; then
#    arrayFonte=(${fonte//"~"/ })
#    dmenos="${arrayFonte[1]}"
#    fonte="${arrayFonte[0]}"
#fi

#if [[ -z $input_start ]]; then
#    startdate="$(date -I -d "$dmenos days ago")"
#else
#    startdate=$(TZ=BRST date -I -d "$input_start") || exit -1
#fi

#if [[ -z $input_end ]]; then
#    enddate="$(date -I -d "$(date -d "$startdate" +'%Y-%m-%d') tomorrow")"
#else
#    enddate=$(TZ=BRST date -I -d "$input_end")     || exit -1
#fi

#if [[ $startdate > $enddate ]]; then
#    echo "Data inicial maior que data final!"
#    exit 134
#fi

#if [[ -z $repalertas ]]; then
#    repalertas="S"
#fi

export SPARK_MAJOR_VERSION=2;
run ()
{
    # Kinit

    # Informa que deve utilizar o spark 2
    export SPARK_MAJOR_VERSION=2;

    now=$(date +%Y%m%d_%H%M%S)
    log=/var/log/ingestao/qualidade/"$1"_"$now"_"$$".log
    #log=/dev/stdout

    timeout --preserve-status --foreground "$3" spark-submit decode-assembly-1.0.jar --fonte "$1" --dtfoto "$2" --repalertas "$4" >> "$log" 2> "$log"
}

spark-submit quality-assembly-1.0.jar --fonte "$1" --base "$2" --file "$3" --dict "$4" --dest "$5"

# Loop das datas
d="$startdate"
while [ "$d" != "$enddate" ]; do

    # Formata a data no estilo que o banco de dados aceita
    day=$(TZ=BRST date -d "$d" +%Y%m%d)
    d=$(TZ=BRST date -I -d "$d + 1 day")
done