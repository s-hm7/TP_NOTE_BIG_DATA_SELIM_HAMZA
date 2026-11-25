# 01 — Environnement Codespace

## Ce que contient le Codespace (constaté au TP2)

| Élément | Où | Remarque |
|---|---|---|
| Hadoop 3.2.1 | `/opt/hadoop` | **Mode local** : pas de namenode, HDFS = disque du Codespace |
| Java 8 | `/usr/lib/jvm/java-8-openjdk-amd64` | Le Java **par défaut peut devenir Java 25** après un redémarrage |
| Jars pour compiler | `lib/hadoop-common-3.2.1.jar`, `lib/hadoop-mapreduce-client-core-3.2.1.jar` | Dans le dépôt |
| Spark | **à vérifier** (voir plus bas) | Nécessaire pour TP3 (Scala) et TP4 (Java) |

## 1. Bloc à coller dans CHAQUE nouveau terminal

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin
```

**Pourquoi** : `PATH` dit à bash où chercher les commandes. Sans `$HADOOP_HOME/bin`, `hadoop` et `hdfs` sont « introuvables » alors qu'ils sont installés. `$JAVA_HOME/bin` est mis **devant** pour gagner sur le Java 25 du système.

### Le rendre permanent (une seule fois)

```bash
cat >> ~/.bashrc << 'EOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin
EOF
source ~/.bashrc
```

## 2. Vérifications (sortie attendue)

```bash
java -version                         # openjdk version "1.8.0_..."
javac -version                        # javac 1.8.0_...
hadoop version                        # Hadoop 3.2.1
which hdfs                            # /opt/hadoop/bin/hdfs
hdfs getconf -confKey fs.defaultFS    # file:///  -> mode local
ls lib                                # les 2 jars Hadoop
```

| `fs.defaultFS` | Signification | Conséquence |
|---|---|---|
| `file:///` | Mode local | `hadoop fs -put f logs/` = copie dans `./logs/`. Chemins **relatifs** au dossier courant |
| `hdfs://localhost:9000` ou `hdfs://namenode:9000` | Vrai HDFS | Il faut un namenode qui tourne, sinon `Connection refused` |

## 3. Spark : vérifier, sinon installer (≈ 3 min)

```bash
which spark-shell spark-submit ; echo $SPARK_HOME
ls /opt | grep -i spark ; ls ~ | grep -i spark
```

Si rien n'apparaît :

```bash
cd ~
wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
tar xzf spark-3.1.2-bin-hadoop3.2.tgz
cat >> ~/.bashrc << 'EOF'
export SPARK_HOME=~/spark-3.1.2-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin
EOF
source ~/.bashrc
cd -                                   # retour dans le dépôt
spark-submit --version                 # version 3.1.2, Scala 2.12, Java 1.8
```

Spark 3.1.2 = la version du cours (`spark-3.1.2-bin-hadoop3.2`). Elle tourne avec **Java 8** : garde le `JAVA_HOME` ci-dessus.

> **À faire la veille** : lancer ces commandes dans le Codespace du TP noté. Si Spark est déjà présent, note juste son chemin.

## 4. Dépannage : symptôme → cause → correction

| Symptôme | Cause | Correction |
|---|---|---|
| `bash: hdfs: command not found` / `hadoop: command not found` | `PATH` perdu (nouveau terminal, redémarrage) | Bloc §1 |
| `java -version` affiche 25 (ou 11, 17…) | Java système par défaut | Bloc §1 (`JAVA_HOME` devant) |
| `UnsupportedClassVersionError … class file version 69.0 … only recognizes up to 52.0` | Compilé avec un Java récent, exécuté en Java 8 | Bloc §1 puis recompiler, ou `javac --release 8 ...` |
| `Connection refused` sur `localhost:9000` | Config HDFS sans namenode | Chemins relatifs, ou `file:///chemin/absolu` |
| `FileAlreadyExistsException: Output directory ... already exists` | Dossier de sortie déjà là | `rm -rf out` (local) / `hadoop fs -rm -r out` |
| `Input path does not exist: file:/workspaces/.../x` | Mauvais dossier ou nom | `hadoop fs -ls` ; respecter majuscules et `.txt` |
| `ClassNotFoundException: exo.ExoDriver` | Mauvais `package.Classe`, ou jar fait depuis le mauvais dossier | `jar tf exo.jar` doit montrer `exo/ExoDriver.class` |
| `error: package org.apache.hadoop.io does not exist` | Classpath absent ou sans guillemets | `javac -cp "lib/*" ...` (guillemets **obligatoires**) |
| `class, interface, or enum expected` / `illegal start of expression` | Accolade `}` en trop ou manquante, ou fichier non enregistré | `Ctrl+S`, compter les `}` |
| `jar` affiche `no such file or directory` | `javac` a échoué avant | Enchaîner avec `&&` |
| `bash: ton_entree: No such file or directory` | `<repère>` copié tel quel | Remplacer par le vrai nom |
| `NumberFormatException` dans le Mapper | En-tête CSV, champ vide, `\r` Windows | `trim()`, `if (key.get()==0) return;`, `try/catch` |
| `ArrayIndexOutOfBoundsException` dans le Mapper | Ligne plus courte que prévu | `if (champs.length < N) return;` |
| Un nouveau terminal « oublie » tout | Les `export` ne valent que pour un terminal | `~/.bashrc` (§1) |

## 5. Commandes utiles autour

```bash
pwd                                    # où suis-je ? (doit être la racine du dépôt)
ls -lh                                 # tailles lisibles
du -sh data/*                          # taille des données
file data/x.csv                        # "with CRLF line terminators" = fins de ligne Windows
head -1 data/x.csv | od -c | tail -3   # voir un \r caché
sed -i 's/\r$//' data/x.csv            # supprimer les \r (sur une copie)
```
