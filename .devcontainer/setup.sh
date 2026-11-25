#!/usr/bin/env bash
# Installe Java 8, Hadoop 3.2.1 (mode local) et Spark 3.1.2. Relançable sans risque.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
HADOOP_V=3.2.1
SPARK_V=3.1.2
JAVA8=/usr/lib/jvm/java-8-openjdk-amd64

echo "==> 1/6 Paquets système (Java 8, outils)"
sudo apt-get update -y -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openjdk-8-jdk-headless curl zip unzip python3 dos2unix git-lfs > /dev/null
sudo update-alternatives --set java  "$JAVA8/jre/bin/java" 2>/dev/null || true
sudo update-alternatives --set javac "$JAVA8/bin/javac"    2>/dev/null || true

echo "==> 2/6 Hadoop $HADOOP_V"
if [ ! -x /opt/hadoop/bin/hadoop ]; then
  curl -fL --retry 5 --retry-delay 5 -o /tmp/hadoop.tgz \
    "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_V/hadoop-$HADOOP_V.tar.gz"
  sudo tar xzf /tmp/hadoop.tgz -C /opt
  sudo rm -rf /opt/hadoop && sudo mv "/opt/hadoop-$HADOOP_V" /opt/hadoop
  sudo rm -rf /opt/hadoop/share/doc /tmp/hadoop.tgz
fi
grep -q "^export JAVA_HOME=$JAVA8" /opt/hadoop/etc/hadoop/hadoop-env.sh \
  || echo "export JAVA_HOME=$JAVA8" | sudo tee -a /opt/hadoop/etc/hadoop/hadoop-env.sh > /dev/null

echo "==> 3/6 Spark $SPARK_V"
if [ ! -x /opt/spark/bin/spark-submit ]; then
  curl -fL --retry 5 --retry-delay 5 -o /tmp/spark.tgz \
    "https://archive.apache.org/dist/spark/spark-$SPARK_V/spark-$SPARK_V-bin-hadoop3.2.tgz"
  sudo tar xzf /tmp/spark.tgz -C /opt
  sudo rm -rf /opt/spark && sudo mv "/opt/spark-$SPARK_V-bin-hadoop3.2" /opt/spark
  sudo rm -f /tmp/spark.tgz
fi
sudo chown -R "$(id -u):$(id -g)" /opt/hadoop /opt/spark

echo "==> 4/6 Variables d'environnement (tous les terminaux)"
if ! grep -q "# >>> tp-note >>>" ~/.bashrc; then
  cat >> ~/.bashrc << BASHRC
# >>> tp-note >>>
export JAVA_HOME=$JAVA8
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export PATH=\$JAVA_HOME/bin:\$PATH:\$HADOOP_HOME/bin:\$SPARK_HOME/bin
[ -f "$REPO/scripts/journal.sh" ] && source "$REPO/scripts/journal.sh"
# <<< tp-note <<<
BASHRC
fi

echo "==> 5/6 Jars Hadoop pour compiler (lib/)"
mkdir -p "$REPO/lib"
cp "/opt/hadoop/share/hadoop/common/hadoop-common-$HADOOP_V.jar" \
   "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-$HADOOP_V.jar" "$REPO/lib/"

echo "==> 6/6 Scripts, garde-fou git"
chmod +x "$REPO"/scripts/*.sh
mkdir -p "$REPO/.git/hooks"
cat > "$REPO/.git/hooks/pre-commit" << 'HOOK'
#!/bin/sh
# Refuse les fichiers de plus de 5 Mo (données, logs) dans un commit
gros=$(git diff --cached --name-only --diff-filter=AM | while read -r f; do
  [ -f "$f" ] && [ "$(stat -c%s "$f")" -gt 5000000 ] && echo "  $f"; done)
if [ -n "$gros" ]; then echo "Commit refusé, fichiers > 5 Mo :"; echo "$gros"; echo "-> git restore --staged <fichier>"; exit 1; fi
exit 0
HOOK
chmod +x "$REPO/.git/hooks/pre-commit"

echo
echo "Installation terminée. Vérification :"
cd "$REPO" && bash scripts/check.sh || true
