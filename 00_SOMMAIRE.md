# Antisèche TP noté — Hadoop · MapReduce · Spark (Codespace)

> Le TP noté porte sur les 4 TP, avec des **problèmes inédits mais proches**.
> À rendre : **le projet** + **un rapport avec toutes les commandes de chaque étape**.

## Les fiches

| Fiche | Contenu | Quand l'ouvrir |
|---|---|---|
| [01_ENVIRONNEMENT](01_ENVIRONNEMENT.md) | Java 8, Hadoop, Spark, `PATH`, dépannage | **En premier**, avant toute chose |
| [02_HDFS](02_HDFS.md) | Commandes `hadoop fs` / `hdfs dfs`, TP1 corrigé | Charger / lire / vérifier des fichiers |
| [03_MAPREDUCE](03_MAPREDUCE.md) | **Modèle Java générique**, 7 recettes, les 5 exos du TP2 | Tout exercice MapReduce |
| [04_SPARK_RDD](04_SPARK_RDD.md) | `spark-shell` Scala, TP3 corrigé | Exercice Spark « RDD » / Scala |
| [05_SPARK_DATAFRAMES](05_SPARK_DATAFRAMES.md) | `AppSpark` Java, API + SQL, TP4 corrigé | Exercice DataFrame / SQL / CSV |
| [06_PROBLEMES_INEDITS](06_PROBLEMES_INEDITS.md) | Problème → clé/valeur → MR / RDD / DF | Face à un sujet jamais vu |
| [07_RAPPORT](07_RAPPORT.md) | Journal automatique des commandes, modèle de rapport | Dès le début (le journal tourne pendant tout le TP) |

## Démarrage : les 3 premières minutes

```bash
# 1. Environnement (détail et dépannage : 01_ENVIRONNEMENT)
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin
java -version          # 1.8.0_xxx   ← sinon STOP, voir 01
hadoop version         # Hadoop 3.2.1
hdfs getconf -confKey fs.defaultFS   # file:/// = mode local (pas de namenode)

# 2. Journal du rapport (détail : 07_RAPPORT)
source antiseches/journal.sh      # active les fonctions j et jn
jn "# Rapport TP noté — Sélim Hamza"
```

## La boucle pour CHAQUE exercice

| # | Étape | Commande type | Pourquoi |
|---|---|---|---|
| 1 | Lire l'énoncé, écrire **la sortie attendue** (1 ligne d'exemple) | — | Sans cible claire, on code dans le vide |
| 2 | **Regarder les données** | `head -3 f` · `wc -l f` · `head -1 f.csv \| tr ';' '\n' \| cat -n` | Le format réel décide du `split` et des index |
| 3 | Choisir **clé** et **valeur** | fiche 06 | C'est 80 % de la note de logique |
| 4 | Créer un **mini-fichier** dont tu connais le résultat | `cat << 'EOF' \| hadoop fs -put - test/t.txt` | Valider la logique en 2 s |
| 5 | Copier le **modèle**, remplir les `TODO` | fiche 03 / 04 / 05 | Structure toujours juste |
| 6 | Compiler → jar → lancer (chaîne `&&`) | fiche 03 | S'arrête à la 1re erreur |
| 7 | **Vérifier sans le programme** | `grep -c` · `awk` · `sort \| uniq -c` | Un job peut réussir et être faux |
| 8 | Journaliser + commit | `j '...'` · `git commit` | Le rapport s'écrit tout seul |

## Les 10 pièges qui coûtent le plus de temps

1. **Java ≠ 8** (Codespace redémarré sur Java 25) → `java -version` avant tout.
2. `hdfs` / `hadoop` : **command not found** → `PATH` perdu (nouveau terminal) → refaire les `export`.
3. **Dossier de sortie déjà existant** → `FileAlreadyExistsException` → `rm -rf out` avant chaque lancement.
4. `javac` échoue mais tu lances quand même le **vieux jar** → toujours enchaîner avec `&&`.
5. **Fichier non enregistré** dans VS Code (point blanc sur l'onglet) → `Ctrl+S`.
6. Accolade manquante → `class, interface, or enum expected` / `illegal start of expression`.
7. Sortie Map ≠ sortie Reduce **sans** `setMapOutputKeyClass/ValueClass` → erreur de type au shuffle.
8. Stocker `val` (objet `Text` réutilisé par Hadoop) au lieu de `val.toString()`.
9. Majuscules dans un nom de fichier (`The_Adventures_of_Sherlock_Holmes.txt`) → `ls` avant `cat`.
10. Copier un `<repère>` dans bash → `bash: x: No such file or directory`.

## Avant de rendre

- [ ] Chaque exercice : code + commandes + sortie (extrait) + vérification indépendante dans le rapport
- [ ] `git status` propre, `git push` fait
- [ ] Les gros fichiers de données ne sont **pas** dans le dépôt (le rapport dit d'où ils viennent)
- [ ] Le rapport explique **le choix clé/valeur** de chaque exercice (c'est ce qui est évalué)
