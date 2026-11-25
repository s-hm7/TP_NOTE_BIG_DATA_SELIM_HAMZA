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
./scripts/check.sh                     # attendu : TOUT EST PRÊT ✅  (sinon fiche 01)
jn "# Journal — TP noté"               # le journal du rapport démarre (fiche 07)
```

## Le dépôt : conventions et raccourcis

| Dossier | Contenu | Dans Git ? |
|---|---|---|
| `data/` | fichiers bruts téléchargés (`cloudera_log.txt`, `arbres.csv`…) | non |
| `hdfs/` | dossiers « HDFS » du mode local : `hdfs/logs`, `hdfs/out_hits`… | non |
| `<exo>/` | 1 exercice MapReduce = 1 dossier = 1 package (`hits/HitsDriver.java`) | oui |
| `scala/` | scripts `spark-shell` | oui |
| `spark/` | `AppSpark.java` (DataFrames) | oui |
| `resultats/` | copie des sorties (remplie par `mr.sh`) | oui |
| `rapport/journal.md` | journal brut des commandes | oui |
| `traces/` | logs complets des jobs | non |

| Raccourci | Fait |
|---|---|
| `./scripts/check.sh` | vérifie Java 8, Hadoop, Spark, un vrai job MR, `spark-submit`, `spark-shell` |
| `./scripts/nouveau.sh mr top` | crée `top/TopDriver.java` depuis le modèle générique |
| `./scripts/nouveau.sh scala hits` | crée `scala/hits.scala` |
| `./scripts/mr.sh top hdfs/in hdfs/out_top [param]` | compile → jar → supprime la sortie → lance → compteurs + extrait + `resultats/top.txt` |
| `./scripts/spark.sh [args]` | compile `spark/*.java` → jar → `spark-submit` |
| `j '...'` / `jn "..."` | journalise une commande / un titre (chargé dans chaque terminal) |

Les scripts **affichent chaque vraie commande** (`$ javac ...`, `$ hadoop jar ...`) : elles vont telles quelles dans le rapport. Les antisèches donnent aussi les commandes à la main, à savoir refaire sans script.

Charger les données d'un exercice :
```bash
hadoop fs -mkdir -p hdfs/logs && hadoop fs -put data/cloudera_log.txt hdfs/logs/ && hadoop fs -ls hdfs/logs
```

## La boucle pour CHAQUE exercice

| # | Étape | Commande type | Pourquoi |
|---|---|---|---|
| 1 | Lire l'énoncé, écrire **la sortie attendue** (1 ligne d'exemple) | — | Sans cible claire, on code dans le vide |
| 2 | **Regarder les données** | `head -3 f` · `wc -l f` · `head -1 f.csv \| tr ';' '\n' \| cat -n` | Le format réel décide du `split` et des index |
| 3 | Choisir **clé** et **valeur** | fiche 06 | C'est 80 % de la note de logique |
| 4 | Créer un **mini-fichier** dont tu connais le résultat | `cat << 'EOF' \| hadoop fs -put - hdfs/test/t.txt` | Valider la logique en 2 s |
| 5 | `./scripts/nouveau.sh mr <nom>`, remplir les `TODO` | fiche 03 / 04 / 05 | Structure toujours juste |
| 6 | `./scripts/mr.sh <nom> hdfs/<in> hdfs/out_<nom>` | fiche 03 | S'arrête à la 1re erreur |
| 7 | **Vérifier sans le programme** | `grep -c` · `awk` · `sort \| uniq -c` | Un job peut réussir et être faux |
| 8 | Journaliser + commit | `j './scripts/mr.sh ...'` · `git add -A && git commit -m ...` | Le rapport s'écrit tout seul |

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
