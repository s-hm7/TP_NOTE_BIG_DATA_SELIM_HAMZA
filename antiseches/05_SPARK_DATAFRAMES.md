# 05 — Spark DataFrames en Java (`AppSpark` + `spark-submit`)

> Code écrit à partir du TP4 et de la doc Spark 3.1, **non exécuté de mon côté** (pas de Spark ici). Compare avec ton code du dépôt `TP4_DataFrameSpark`, qui lui a tourné.

## 1. La chaîne complète

```bash
# spark/AppSpark.java  (package spark)
javac -cp "$SPARK_HOME/jars/*" -d . spark/AppSpark.java \
 && jar cvf appspark.jar spark/*.class > /dev/null \
 && spark-submit --class spark.AppSpark appspark.jar 2>&1 | tee traces/appspark.log
```
Arguments du programme : **après** le jar → `spark-submit --class spark.AppSpark appspark.jar 2026 11`.
`$SPARK_HOME` vide ? → fiche 01 §3. Énoncé Docker : `/opt/spark-3.1.2/bin/spark-submit --class spark.AppSpark /appspark.jar`.

## 2. Modèle générique `AppSpark.java`

```java
package spark;

import static org.apache.spark.sql.functions.*;        // col, lower, avg, count, when, desc, split, explode...

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppSpark {

    // Lancer spark-submit depuis la racine du dépôt : "data/x.csv" -> file:///workspaces/<repo>/data/x.csv
    static String chemin(String relatif) {
        return "file://" + System.getProperty("user.dir") + "/" + relatif;
    }

    static Dataset<Row> lireCsv(SparkSession spark, String relatif) {
        return spark.read()
                .option("header", "true")        // 1re ligne = noms de colonnes
                .option("inferSchema", "true")   // devine int / double / string
                .option("sep", ";")              // séparateur des CSV opendata Paris
                .csv(chemin(relatif));
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder()
                .appName("AppSpark")
                .master("local[*]")
                .getOrCreate();
        System.out.println("Spark version : " + spark.version());

        Dataset<Row> df = lireCsv(spark, "data/arbres.csv");
        df.printSchema();                                   // noms EXACTS + types des colonnes
        System.out.println("Lignes : " + df.count());
        df.show(5, false);

        // ===== Q1 ... ton pipeline ici
        System.out.println("---------- Q1 ----------");

        spark.stop();
    }
}
```

## 3. Antisèche API Java

| Besoin | Java |
|---|---|
| Colonnes | `col("NOM")` — nom **exact**, espaces et parenthèses compris |
| Choisir | `df.select("A", "B")` · `df.select(col("A"), col("B").alias("b"))` |
| Égal / différent | `col("A").equalTo("x")` · `col("A").notEqual("x")` — **jamais `==`** |
| Comparer | `.gt(0)` `.geq(0)` `.lt(10)` `.leq(10)` `.between(630, 1410)` |
| Et / ou / non | `.and(...)` `.or(...)` `not(...)` |
| Liste | `col("A").isin("a", "b", "c")` |
| Texte | `lower(col("A"))` · `.contains("x")` · `.like("%x%")` · `trim(col("A"))` |
| Nul | `.isNull()` · `.isNotNull()` · `df.na().drop()` |
| Filtrer | `df.filter(cond)` ( = `where`) |
| Nouvelle colonne | `df.withColumn("c", expr)` |
| Renommer | `df.withColumnRenamed("count", "nombre")` |
| Conversion | `col("A").cast("double")` / `"int"` |
| Si / sinon | `when(cond, v).otherwise(w)` — sans `otherwise` : `null` |
| Grouper + compter | `df.groupBy("A").count()` → colonne **`count`** |
| Plusieurs agrégats | `df.groupBy("A").agg(avg("B").alias("moy"), max("C"), countDistinct("D"))` |
| Trier | `orderBy(col("count").desc())` · `orderBy(desc("count"))` · `orderBy("A")` |
| Distinct | `df.select("A").distinct()` |
| Limiter | `df.limit(10)` · `show(10, false)` (`false` = ne pas tronquer) |
| Compter | `df.count()` → `long` |
| Jointure | `a.join(b, a.col("id").equalTo(b.col("id")))` · `a.join(b, "id")` |
| Mots d'un texte | `spark.read().text(chemin("f"))` → 1 colonne `value` ; `explode(split(col("value"), "\\W+"))` |
| Regex | `regexp_extract(col("value"), ":(\\d\\d):(\\d\\d):", 1)` |
| Dates | `to_date(col("d"))` · `year(...)` · `month(...)` |
| Sauver | `res.coalesce(1).write().mode("overwrite").option("header", "true").option("sep", ";").csv(chemin("resultats/q9"))` |

### SQL
```java
df.createOrReplaceTempView("arbres");
spark.sql("SELECT `LIBELLE FRANCAIS` AS arbre, COUNT(*) AS nb FROM arbres GROUP BY `LIBELLE FRANCAIS` ORDER BY nb DESC").show(10, false);
```
Noms avec espaces / parenthèses → **backticks** `` ` `` (AltGr+7). Chaînes → apostrophes `'...'`.

## 4. TP4 corrigé

```java
// Noms : à recopier depuis printSchema() (les exports récents utilisent parfois libelle_francais, hauteur_m...)
static final String LIB  = "LIBELLE FRANCAIS";
static final String DOM  = "DOMANIALITE";
static final String CIRC = "CIRCONFERENCE (cm)";
static final String HAUT = "HAUTEUR (m)";
```

```java
// Q6 — charger, schéma, nombre
Dataset<Row> arbres = lireCsv(spark, "data/arbres.csv");
arbres.printSchema();
System.out.println("Nombre d'arbres : " + arbres.count());

// Q7 — abricotiers
long abricotiers = arbres.filter(lower(col(LIB)).equalTo("abricotier")).count();
System.out.println("Abricotiers : " + abricotiers);

// Q8 — arbre le plus planté dans les écoles
arbres.groupBy(DOM).count().show(false);                    // d'abord : quelle valeur = école ?
arbres.filter(col(DOM).equalTo("DASCO"))                    // DASCO = Direction des affaires scolaires (à confirmer)
      .groupBy(LIB).count()
      .orderBy(col("count").desc())
      .show(1, false);

// Q9 — 4 essences, colonne count renommée
Dataset<Row> quatre = arbres.filter(lower(col(LIB)).isin("tilleul", "bouleau", "platane", "peuplier"));
quatre.groupBy(LIB).count().withColumnRenamed("count", "nombre").show();

// Q10 — moyennes sans les 0 : when() sans otherwise donne null, et avg ignore les null
quatre.groupBy(LIB)
      .agg(avg(when(col(CIRC).gt(0), col(CIRC))).alias("circonference_moy"),
           avg(when(col(HAUT).gt(0), col(HAUT))).alias("hauteur_moy"))
      .show();
```

```java
// Q11 — les mêmes en SQL
arbres.createOrReplaceTempView("arbres");
spark.sql("SELECT COUNT(*) AS nb FROM arbres WHERE lower(`LIBELLE FRANCAIS`) = 'abricotier'").show();
spark.sql("SELECT `LIBELLE FRANCAIS`, COUNT(*) AS nb FROM arbres WHERE DOMANIALITE = 'DASCO' "
        + "GROUP BY `LIBELLE FRANCAIS` ORDER BY nb DESC LIMIT 1").show(false);
spark.sql("SELECT `LIBELLE FRANCAIS`, COUNT(*) AS nombre FROM arbres "
        + "WHERE lower(`LIBELLE FRANCAIS`) IN ('tilleul','bouleau','platane','peuplier') "
        + "GROUP BY `LIBELLE FRANCAIS`").show();
spark.sql("SELECT `LIBELLE FRANCAIS`, "
        + "AVG(CASE WHEN `CIRCONFERENCE (cm)` > 0 THEN `CIRCONFERENCE (cm)` END) AS circ_moy, "
        + "AVG(CASE WHEN `HAUTEUR (m)` > 0 THEN `HAUTEUR (m)` END) AS haut_moy "
        + "FROM arbres WHERE lower(`LIBELLE FRANCAIS`) IN ('tilleul','bouleau','platane','peuplier') "
        + "GROUP BY `LIBELLE FRANCAIS`").show();
```

```java
// Q12 — événements d'un mois + station Vélib la plus proche
// imports en plus : org.apache.spark.sql.expressions.Window ; org.apache.spark.sql.expressions.WindowSpec
int annee = Integer.parseInt(args[0]), mois = Integer.parseInt(args[1]);
String EV_TITRE = "title", EV_DATE = "date_start", EV_GEO = "lat_lon";     // ⚠️ printSchema()
String V_NOM = "name", V_GEO = "coordonnees_geo";                         // ⚠️ printSchema()

Dataset<Row> ev = lireCsv(spark, "data/evenements.csv")
        .filter(year(col(EV_DATE)).equalTo(annee).and(month(col(EV_DATE)).equalTo(mois)))
        .filter(col(EV_GEO).isNotNull())
        .withColumn("ev_lat", split(col(EV_GEO), ",\\s*").getItem(0).cast("double"))
        .withColumn("ev_lon", split(col(EV_GEO), ",\\s*").getItem(1).cast("double"));
Dataset<Row> velib = lireCsv(spark, "data/velib.csv")
        .withColumn("v_lat", split(col(V_GEO), ",\\s*").getItem(0).cast("double"))
        .withColumn("v_lon", split(col(V_GEO), ",\\s*").getItem(1).cast("double"));

double k = Math.cos(Math.toRadians(48.86));          // 1° de longitude est plus court à Paris
Dataset<Row> paires = ev.crossJoin(velib).withColumn("dist",
        pow(col("ev_lat").minus(col("v_lat")), 2)
        .plus(pow(col("ev_lon").minus(col("v_lon")).multiply(k), 2)));

WindowSpec w = Window.partitionBy(col(EV_TITRE), col(EV_DATE)).orderBy(col("dist"));
paires.withColumn("rang", row_number().over(w))
      .filter(col("rang").equalTo(1))                // la station la plus proche
      .select(EV_TITRE, EV_DATE, V_NOM)
      .orderBy(EV_DATE)
      .show(100, false);
```
`crossJoin` = chaque événement × chaque station (≈ 1 500 stations) : acceptable pour un mois d'événements.

## 5. Les exercices MapReduce refaits en DataFrame (pour un sujet inédit)

```java
// WordCount
Dataset<Row> mots = spark.read().text(chemin("gutenberg"))
        .select(explode(split(lower(col("value")), "[^\\p{L}]+")).alias("mot"))
        .filter(length(col("mot")).gt(0));
mots.groupBy("mot").count().orderBy(col("count").desc()).show(20, false);

// Hits par quart d'heure
String re = ":(\\d\\d):(\\d\\d):\\d\\d";
spark.read().text(chemin("logs"))
     .select(regexp_extract(col("value"), re, 1).cast("int").alias("h"),
             regexp_extract(col("value"), re, 2).cast("int").alias("m"))
     .withColumn("q", col("h").multiply(60).plus(floor(col("m").divide(15)).multiply(15)))
     .filter(col("q").between(630, 1410))
     .groupBy("q").count().orderBy("q")
     .select(format_string("%02d:%02d", floor(col("q").divide(60)), col("q").mod(60)).alias("tranche"), col("count"))
     .show(60, false);

// Compter des éléments différents (≈ exo 5)
df.groupBy("produit").agg(countDistinct("client").alias("nb_clients")).show();
```

## 6. Pièges DataFrame

| Piège | Symptôme | Correction |
|---|---|---|
| Nom de colonne approximatif | `AnalysisException: cannot resolve 'X' given input columns: [...]` | Copier le nom depuis la liste du message / `printSchema()` |
| `sep` oublié | 1 seule colonne contenant tous les `;` | `.option("sep", ";")` |
| `header` oublié | Colonnes `_c0`, `_c1`… | `.option("header", "true")` |
| Nombre lu comme `string` (virgule décimale, valeurs vides) | `avg` donne `null` ou erreur | `regexp_replace(col(X), ",", ".").cast("double")` |
| `==` entre colonnes | Ne compile pas / toujours faux | `equalTo` |
| `count()` vs `groupBy().count()` | Le 1er rend un `long`, le 2nd un DataFrame avec colonne `count` | |
| `show()` tronque à 20 caractères | Libellés coupés | `show(n, false)` |
| Chemin relatif | `Path does not exist` | Lancer depuis la racine du dépôt, ou `file:///` absolu |
| `write()` sur un dossier existant | `already exists` | `.mode("overwrite")` |
| Logs illisibles | Des centaines de lignes INFO | `Logger.getLogger("org").setLevel(Level.ERROR)` |
