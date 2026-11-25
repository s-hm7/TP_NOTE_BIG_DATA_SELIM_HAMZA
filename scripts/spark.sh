#!/usr/bin/env bash
# Usage : ./scripts/spark.sh [Classe] [args ...]      (Classe par défaut : AppSpark, dans spark/)
# Ex.   : ./scripts/spark.sh                 ./scripts/spark.sh AppSpark 2026 11
set -u
cd "$(dirname "$0")/.."
classe=AppSpark
if [ $# -gt 0 ] && [ -f "spark/$1.java" ]; then classe=$1; shift; fi
run() { printf '$'; printf ' %q' "$@"; echo; "$@"; }
mkdir -p traces resultats
run javac -cp "$SPARK_HOME/jars/*" -d . spark/*.java || { echo "!! Compilation échouée"; exit 1; }
run jar cf spark.jar spark/*.class || exit 1
printf '$ spark-submit --class spark.%s spark.jar' "$classe"; [ $# -gt 0 ] && printf ' %q' "$@"; echo " 2>&1 | tee traces/spark_$classe.log"
spark-submit --class "spark.$classe" spark.jar "$@" 2>&1 | tee "traces/spark_$classe.log"
exit "${PIPESTATUS[0]}"
