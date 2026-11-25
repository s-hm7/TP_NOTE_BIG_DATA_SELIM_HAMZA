#!/usr/bin/env bash
# Vérifie tout l'environnement en ~1 min : Java 8, Hadoop, jars, job MapReduce réel, Spark Java, spark-shell.
REPO="$(cd "$(dirname "$0")/.." && pwd)"
ECHECS=0
ok() { echo "  ✅ $1"; }
ko() { echo "  ❌ $1"; ECHECS=$((ECHECS+1)); }

echo "== Outils"
java -version 2>&1 | grep -q '"1\.8' && ok "java 8" || ko "java : $(java -version 2>&1 | head -1)  -> voir antiseches/01"
javac -version 2>&1 | grep -q ' 1\.8' && ok "javac 8" || ko "javac : $(javac -version 2>&1)"
hadoop version 2>/dev/null | head -1 | grep -q '3\.2\.1' && ok "hadoop 3.2.1" || ko "hadoop introuvable (PATH ?)"
fs=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null)
[ "$fs" = "file:///" ] && ok "HDFS en mode local (file:///)" || ko "fs.defaultFS = '$fs'"
ls "$REPO"/lib/hadoop-common-*.jar "$REPO"/lib/hadoop-mapreduce-client-core-*.jar > /dev/null 2>&1 \
  && ok "lib/ : jars Hadoop" || ko "lib/ vide -> bash .devcontainer/setup.sh"
spark-submit --version 2>&1 | grep -q 'version 3\.1\.2' && ok "spark 3.1.2" || ko "spark-submit introuvable"

T=$(mktemp -d)
echo "== Job MapReduce réel (modèle générique)"
( cd "$T" && mkdir -p exo entree \
  && cp "$REPO/modeles/ExoDriver.java.txt" exo/ExoDriver.java \
  && printf 'a;1\nb;2\na;3\n' > entree/t.csv \
  && javac -cp "$REPO/lib/*" -d . exo/ExoDriver.java \
  && jar cf exo.jar exo/*.class \
  && hadoop jar exo.jar exo.ExoDriver entree sortie > job.log 2>&1 \
  && grep -q "^a	2$" sortie/part-r-00000 ) && ok "compilation + jar + hadoop jar (a=2, b=1)" || ko "job MapReduce : voir $T/job.log"

echo "== Spark Java (spark-submit)"
mkdir -p "$T/sp" && cat > "$T/sp/VerifSpark.java" << 'JAVA'
import org.apache.spark.sql.SparkSession;
public class VerifSpark {
  public static void main(String[] a) {
    SparkSession s = SparkSession.builder().appName("verif").master("local[*]").getOrCreate();
    System.out.println("SPARK_JAVA_OK " + s.range(10).count());
    s.stop();
  }
}
JAVA
( cd "$T/sp" && javac -cp "$SPARK_HOME/jars/*" -d . VerifSpark.java && jar cf v.jar VerifSpark.class \
  && spark-submit --class VerifSpark v.jar 2>/dev/null | grep -q "SPARK_JAVA_OK 10" ) \
  && ok "javac + spark-submit (range(10).count = 10)" || ko "spark-submit Java"

echo "== Spark Scala (spark-shell)"
echo 'println("SCALA_OK " + sc.parallelize(1 to 100).sum.toInt)' | spark-shell 2>/dev/null | grep -q "SCALA_OK 5050" \
  && ok "spark-shell (somme 1..100 = 5050)" || ko "spark-shell"

rm -rf "$T"
echo
[ $ECHECS -eq 0 ] && echo "TOUT EST PRÊT ✅" || echo "$ECHECS problème(s) ❌ — antiseches/01_ENVIRONNEMENT.md"
exit $ECHECS
