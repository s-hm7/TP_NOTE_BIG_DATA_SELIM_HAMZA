# 04 — Spark RDD en Scala (`spark-shell`)

> Code écrit à partir du TP3 et du cours, **non exécuté de mon côté** (pas de Spark dans mon environnement). Teste chaque bloc sur un petit fichier avant le gros.

## 1. Lancer et se repérer

```bash
spark-shell                      # prompt scala>   (sortie : :quit ou Ctrl+D)
```
```scala
sc.setLogLevel("ERROR")                                // moins de bruit
sc.hadoopConfiguration.get("fs.defaultFS")             // "file:///" -> chemins relatifs au dossier de lancement
```

| Chemin | Quand |
|---|---|
| `sc.textFile("logs")` | Codespace, `spark-shell` lancé à la racine du dépôt |
| `sc.textFile("file:///workspaces/<repo>/logs")` | Sûr dans tous les cas |
| `sc.textFile("hdfs://localhost:9000/user/root/logs")` | Énoncé TP3, vrai cluster (dans Docker : `hdfs://namenode:9000`) |

Un **dossier** = tous ses fichiers (sauf ceux qui commencent par `.` ou `_`).

## 2. ⚠️ Le piège n°1 : le code sur plusieurs lignes

Le shell exécute **chaque ligne dès qu'elle est complète**. Une ligne qui commence par `.filter(...)` provoque une erreur.

| Solution | Comment |
|---|---|
| **Mode collage** | `:paste` → coller le bloc → **Ctrl+D** |
| **Fichier** (idéal pour le rendu) | écrire `scala/hits.scala`, puis dans le shell `:paste scala/hits.scala` |
| Une seule ligne | tout enchaîner sur une ligne |

→ Garde un fichier `.scala` par exercice dans le dépôt : c'est ton code à rendre, et tu le relances en une commande.

## 3. Transformations (paresseuses) et actions (déclenchent le calcul)

| Transformation | Effet | Action | Effet |
|---|---|---|---|
| `map(f)` | 1 → 1 | `count()` | nombre d'éléments |
| `flatMap(f)` | 1 → n | `collect()` | tout ramener (petit résultat seulement) |
| `filter(p)` | garde si vrai | `take(n)` / `first()` | n premiers |
| `distinct()` | sans doublon | `reduce(f)` | agrège tout |
| `reduceByKey(f)` | agrège par clé | `countByKey()` / `countByValue()` | Map clé → nombre |
| `groupByKey()` | clé → liste | `saveAsTextFile("out")` | dossier de sortie (ne doit pas exister) |
| `sortByKey(asc)` / `sortBy(f, asc)` | tri | `foreach(println)` | afficher |
| `mapValues(f)` | transforme la valeur | | |
| `join(autreRdd)` | jointure sur la clé | | |
| `zipWithIndex()` | (élément, index à partir de 0) | | |

**Rien n'est calculé avant une action** (évaluation paresseuse) : une erreur dans un `map` n'apparaît qu'au `count()` / `take()`.
**`reduceByKey` > `groupByKey`** pour une somme : il pré-agrège avant le shuffle (comme un combiner).

### Correspondance MapReduce ↔ Spark

| MapReduce | Spark RDD |
|---|---|
| Mapper qui émet n couples | `flatMap` puis `map(x => (cle, valeur))` |
| `return` dans le Mapper | `filter` |
| Reducer somme | `reduceByKey(_ + _)` |
| Reducer `TreeSet` | `groupByKey()` + `.toList.distinct.sorted` |
| Reducer `HashSet.size()` | `distinct()` puis `map((cle, 1)).reduceByKey(_ + _)` |
| `part-r-00000` | `saveAsTextFile` → `part-00000`, `part-00001`… |

## 4. TP3 corrigé

### Premières manipulations
```scala
println("Bonjour à tous !")
val rdd_gut = sc.textFile("gutenberg")
rdd_gut.count()                                          // nb de lignes
rdd_gut.take(3).foreach(println)                         // 3 premières lignes
rdd_gut.filter(_.contains("murderer")).count()           // lignes contenant "murderer"
rdd_gut.map(_.length.toLong).reduce(_ + _)               // nb de caractères (hors \n)
```

### WordCount en 3 étapes, puis en 1 ligne
```scala
val mots   = rdd_gut.flatMap(line => line.split("\\W+"))   // 1 ligne -> n mots
val paires = mots.filter(_.nonEmpty).map(m => (m, 1))      // = MAP
val compte = paires.reduceByKey(_ + _)                     // = REDUCE
compte.take(10).foreach(println)

rdd_gut.flatMap(_.split("\\W+")).filter(_.nonEmpty).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(20).foreach(println)
```

### WordCount nettoyé (numéros, actes/scènes, didascalies entre crochets)
```scala
rdd_gut.filter(_.contains("[")).take(5).foreach(println)                    // regarder d'abord
val propre = rdd_gut
  .map(_.replaceAll("\\[[^\\]]*\\]", " "))            // supprime [Exit.] etc. (sur une même ligne)
  .filter(l => !l.trim.matches("(ACT|SCENE)\\s.*"))   // titres d'actes et de scènes
val wc2 = propre
  .flatMap(_.split("\\W+"))
  .filter(w => w.nonEmpty && !w.matches("\\d+"))      // numéros de pages / scènes
  .map((_, 1)).reduceByKey(_ + _)
wc2.sortBy(_._2, false).take(20).foreach(println)
```
Limite à signaler dans le rapport : un crochet ouvert sur une ligne et fermé sur la suivante n'est pas supprimé.

### Anagrammes → HDFS
```scala
import java.text.Normalizer
def cle(m: String) = Normalizer.normalize(m, Normalizer.Form.NFD).replaceAll("\\p{M}", "").sorted

val anag = sc.textFile("mots")
  .flatMap(_.toLowerCase.split("[^\\p{L}]+"))
  .filter(_.nonEmpty)
  .distinct()                                           // = le TreeSet du Reducer
  .map(m => (cle(m), m))
  .groupByKey()
  .filter({ case (k, v) => v.size > 1 })                // aide de l'énoncé
  .map({ case (k, v) => k + " : " + v.toList.sorted.mkString(" ") })
anag.collect().foreach(println)                         // attendu : 17 groupes (comme en MapReduce)
anag.saveAsTextFile("anagrammes_spark")
```

### Hits par quart d'heure (10h30 → tranche 23:30 incluse, comme le corrigé)
```scala
val hits = sc.textFile("logs")
  .map(l => (":\\d\\d:\\d\\d:\\d\\d".r findFirstIn l).getOrElse(""))   // aide de l'énoncé -> ":10:30:13"
  .filter(_.nonEmpty)
  .map(t => { val h = t.substring(1, 3).toInt; val m = t.substring(4, 6).toInt; h * 60 + m / 15 * 15 })
  .filter(q => q >= 10 * 60 + 30 && q <= 23 * 60 + 30)
  .map(q => (q, 1))
  .reduceByKey(_ + _)
  .sortByKey()
  .map({ case (q, n) => f"${q / 60}%02d:${q % 60}%02d $n" })
hits.collect().foreach(println)            // attendu : 53 lignes, 10:30 2486 ... 23:30 7243
```

### Index d'un mot passé en paramètre
```bash
MOT=burglar spark-shell                    # le "paramètre" passe par une variable d'environnement
```
```scala
val mot = sys.env.getOrElse("MOT", "gentleman")
val lignes = sc.textFile("sherlock")       // sherlock_num.txt produit par cat -n
  .map(_.split("\t", 2))
  .filter(_.length == 2)
  .filter(p => p(1).split("\\W+").exists(_.equalsIgnoreCase(mot)))
  .map(p => p(0).trim.toInt)
  .distinct()
  .sortBy(identity)
println(mot + " " + lignes.count() + " " + lignes.collect().mkString("[", ", ", "]"))
// attendus : burglar 1 · murderer 5 · bachelor 8 · gentleman 45 · innocent 17
```
Sans `cat -n` : `sc.textFile("The_Adventures_of_Sherlock_Holmes.txt").zipWithIndex()` → `(ligne, i)`, numéro = `i + 1`.

### Arbres de Paris (CSV `;`)
```scala
val brut   = sc.textFile("data/arbres.csv")
val entete = brut.first()
entete.split(";").zipWithIndex.foreach(println)          // repérer les index des colonnes
val ok = brut.filter(_ != entete).map(_.split(";", -1))  // -1 : garde les champs vides
// ⚠️ remplacer par les index affichés juste au-dessus
val iArr = 3; val iLib = 8; val iCirc = 12; val iHaut = 13
val lignes = ok.filter(_.length > iHaut)

// 1. Nombre de variétés par arrondissement, du plus riche au moins riche
lignes.map(c => (c(iArr), c(iLib))).filter(_._2.nonEmpty)
  .distinct()                                            // 1 fois par (arrondissement, variété)
  .map({ case (arr, _) => (arr, 1) }).reduceByKey(_ + _)
  .sortBy(_._2, false).collect().foreach(println)

// 2. Circonférence et hauteur moyennes par type, sans les zéros
def num(s: String) = scala.util.Try(s.trim.replace(',', '.').toDouble).getOrElse(0.0)
lignes.map(c => (c(iLib), (num(c(iCirc)), num(c(iHaut)))))
  .filter({ case (_, (ci, h)) => ci > 0 && h > 0 })
  .mapValues({ case (ci, h) => (ci, h, 1) })
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))   // sommes + effectif
  .mapValues({ case (sc_, sh, n) => (sc_ / n, sh / n) })
  .collect().foreach(println)
```
« Variété » est ambigu (libellé français, espèce, ou colonne variété) : **écris ton choix dans le rapport**.
Choix « sans zéros » : j'écarte l'arbre si **l'une des deux** mesures vaut 0 ; l'autre option (écarter chaque mesure séparément) est défendable aussi.

## 5. Recettes RDD pour problèmes inédits

```scala
val r = sc.textFile("data/f.csv").map(_.split(";", -1)).filter(_.length >= 3)   // base CSV

r.map(c => (c(0), 1)).reduceByKey(_ + _)                                  // compter par clé
r.map(c => (c(0), c(1))).distinct().map(x => (x._1, 1)).reduceByKey(_ + _)  // nb de c(1) DIFFÉRENTS par c(0)
r.map(c => (c(0), c(2).toInt)).reduceByKey(math.max(_, _))                // max par clé
r.map(c => (c(0), (c(2).toDouble, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
 .mapValues({ case (s, n) => s / n })                                     // moyenne par clé
r.map(c => (c(0), 1)).reduceByKey(_ + _).sortBy(_._2, false).take(10)     // top 10
val a = r.map(c => (c(0), c(1))); val b = sc.textFile("data/g.csv").map(_.split(";")).map(c => (c(0), c(1)))
a.join(b)                                                                 // (cle, (va, vb))
r.map(_.mkString(";")).saveAsTextFile("resultats/q1")                     // sauver
```

## 6. Pièges Spark/Scala

| Piège | Correction |
|---|---|
| Ligne commençant par `.` dans le shell | `:paste` puis Ctrl+D |
| `saveAsTextFile` : `already exists` | `rm -rf resultats/q1` avant |
| `collect()` sur 180 000 lignes | `take(20)` pour regarder |
| `split(";")` perd les champs vides finaux | `split(";", -1)` |
| `.toInt` sur un champ vide → `NumberFormatException` au moment de l'**action** | `scala.util.Try(...).getOrElse(0)` |
| En-tête CSV traité comme une donnée | `filter(_ != entete)` |
| `"a" == "a"` | En Scala `==` compare bien le contenu (≠ Java) |
