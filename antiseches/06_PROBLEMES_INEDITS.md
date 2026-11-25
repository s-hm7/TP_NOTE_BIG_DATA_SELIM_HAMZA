# 06 — Face à un problème inédit

## 1. Lire l'énoncé en 4 questions (à écrire dans le rapport)

| Question | Exemple (exo 5) |
|---|---|
| **Une ligne d'entrée** ressemble à quoi ? | `c1;A;p1;p2;p3` |
| **Une ligne de sortie** ressemble à quoi ? | `p1,p2   2` |
| Sur **quoi** je regroupe ? → **la clé** | le couple `p1,p2` |
| **Que** j'agrège, et **comment** ? → **la valeur** + le Reduce | les clients → nombre de clients **différents** |

Mots de l'énoncé → opération :

| L'énoncé dit… | Opération |
|---|---|
| « nombre de », « combien » | somme de `1` |
| « différents », « distincts » | `HashSet` / `distinct` / `countDistinct` |
| « liste des », « tous les » | `TreeSet` / `groupByKey` / `collect_list` |
| « moyenne » | somme / effectif (pas de combiner) |
| « le plus », « le moins », « top » | tri + limite (`cleanup`, `sortBy(..).take`, `orderBy().limit`) |
| « par heure / quart d'heure / mois » | clé = tranche calculée (division entière) |
| « entre X et Y », « sauf », « seulement » | filtre (`return` / `filter`) |
| « passé en paramètre » | `args[2]` → `Configuration` (MR) · variable (Scala) · `args[0]` (Java Spark) |
| « pour tout couple » | double boucle `i < j` sur une liste triée |
| croiser 2 fichiers | jointure (clé commune) |

## 2. Table de traduction : problème → clé / valeur → 3 technos

| Problème type | Clé (Map) | Valeur | Reduce | RDD | DataFrame |
|---|---|---|---|---|---|
| Compter par X | X | 1 | somme | `map((x,1)).reduceByKey(_+_)` | `groupBy(X).count()` |
| Nb de Y différents par X | X | Y | `HashSet.size()` | `map((x,y)).distinct().map((_._1,1)).reduceByKey(_+_)` | `groupBy(X).agg(countDistinct(Y))` |
| Lister les Y par X | X | Y | `TreeSet` → texte | `groupByKey().mapValues(_.toList.distinct.sorted)` | `groupBy(X).agg(collect_set(Y))` |
| Moyenne de V par X | X | V | somme / n | `mapValues((_,1)).reduceByKey(...)` | `groupBy(X).agg(avg(V))` |
| Max de V par X | X | V | max | `reduceByKey(math.max(_,_))` | `groupBy(X).agg(max(V))` |
| Top N des X | X | 1 | somme + tri dans `cleanup` | `...reduceByKey(_+_).sortBy(_._2,false).take(N)` | `groupBy(X).count().orderBy(desc("count")).limit(N)` |
| Regrouper par clé calculée (anagrammes) | f(élément) | élément | `TreeSet`, garder si ≥ 2 | `map(e=>(f(e),e)).groupByKey().filter(_._2.size>1)` | `withColumn("k", f).groupBy("k").agg(collect_set(...))` |
| Par tranche de temps | `h*60 + m/15*15` | 1 | somme | idem compter | `regexp_extract` + `groupBy` |
| Index d'un mot | mot | n° de ligne | `TreeSet<Integer>` | `filter(...).map(n° ligne)` | `filter(...).select(n°)` |
| Couples (x,y) | `"x,y"` (x<y) | qui | `HashSet.size()` | `flatMap(couples).distinct()...` | `explode` + self-join, ou RDD |
| Total global | `"total"` | V | somme | `map(V).reduce(_+_)` | `agg(sum(V))` |
| Jointure A ⋈ B | id commun | étiquette + donnée | sépare A et B | `a.join(b)` | `a.join(b, "id")` |
| Filtre seul | ligne | — | aucun (map-only) | `filter` | `filter` |

## 3. Variantes probables, avec la clé déjà choisie

Logs Apache (`cloudera_log.txt`) — après `split(" ")` :

| Index | Exemple | Champ |
|---|---|---|
| `[0]` | `79.133.215.123` | IP du client |
| `[3]` | `[14/Jun/2014:10:30:13` | date + heure (→ `split(":")` : `[1]` heure, `[2]` minutes) |
| `[5]` | `"GET` | méthode (avec le guillemet) |
| `[6]` | `/department/fitness` | URL demandée |
| `[8]` | `200` | code HTTP |
| `[9]` | `1671` | taille en octets |

| Sujet possible | Clé | Valeur | Reduce |
|---|---|---|---|
| Hits par **heure** | `t[1]` (heure) | 1 | somme |
| Hits par **page** / top 10 pages | `champs[6]` | 1 | somme (+ top) |
| Nb d'**IP différentes** par heure | heure | IP | `HashSet.size()` |
| Pages en **erreur 404** | `champs[6]` si `champs[8]` = `404` | 1 | somme |
| Octets servis par heure | heure | `champs[9]` (ignorer `-`) | somme (`LongWritable`) |
| Pages vues par **département** | `champs[6].split("/")[2]` si URL commence par `/department/` | 1 | somme |

Texte (Shakespeare / Sherlock) :

| Sujet possible | Clé | Valeur | Reduce |
|---|---|---|---|
| Mots de plus de N lettres les plus fréquents | mot (si `length() > N`) | 1 | somme + top |
| Nombre de mots par **longueur** | `String.valueOf(w.length())` | 1 | somme |
| Index de **tous** les mots | mot | n° de ligne | `TreeSet<Integer>` |
| Index de **plusieurs** mots en paramètre | mot (si dans le `Set` lu en `setup`) | n° de ligne | `TreeSet<Integer>` |
| Mots par **initiale** | `w.substring(0,1)` | mot | `TreeSet` |
| Palindromes | `"palindromes"` (si `w` = son inverse) | mot | `TreeSet` |

Commandes (`id_cde;id_client;produits…`) :

| Sujet possible | Clé | Valeur | Reduce |
|---|---|---|---|
| Nb de commandes par client | client | 1 | somme |
| Nb de produits **différents** par client | client | produit | `HashSet.size()` |
| Produit le plus commandé | produit | 1 | somme + top 1 |
| Taille moyenne d'une commande | `"moyenne"` | nb de produits | somme / n |
| Clients ayant acheté le produit P (paramètre) | P | client | `TreeSet` |

Arbres / CSV opendata :

| Sujet possible | Clé | Valeur | Reduce |
|---|---|---|---|
| Nb d'arbres par arrondissement | arrondissement | 1 | somme |
| Hauteur max par espèce | espèce | hauteur | max |
| Arbres remarquables par arrondissement | arrondissement (si remarquable) | 1 | somme |

## 4. Plusieurs paramètres en MapReduce

```java
// Driver : hadoop jar x.jar exo.ExoDriver in out burglar,murderer,bachelor
job.getConfiguration().set("mots", args[2]);

// Mapper
private final HashSet<String> mots = new HashSet<String>();
@Override
protected void setup(Context context) throws IOException, InterruptedException {
    for (String m : context.getConfiguration().get("mots").split(",")) mots.add(m.trim().toLowerCase());
}
// dans map : if (mots.contains(w.toLowerCase())) context.write(new Text(w.toLowerCase()), new IntWritable(numero));
```
Import en plus : `java.util.HashSet`.

## 5. Vérifier SANS le programme (preuve pour le rapport)

| Vérification | Commande |
|---|---|
| Lignes contenant X | `grep -c "X" f` · mot entier, sans casse : `grep -ciw "x" f` |
| Compter par colonne 2 (CSV `;`) | `cut -d';' -f2 f \| sort \| uniq -c \| sort -rn \| head` |
| Nb de valeurs différentes | `cut -d';' -f2 f \| sort -u \| wc -l` |
| Somme d'une colonne | `awk -F';' '{s+=$3} END{print s}' f` |
| Moyenne sans les 0 | `awk -F';' '$3>0{s+=$3;n++} END{print s/n}' f` |
| Max | `awk -F';' 'NR>1 && $3>m{m=$3} END{print m}' f` |
| Total d'une sortie MR = nb de lignes d'entrée ? | `awk '{s+=$2} END{print s}' out/part-r-00000` |
| Trier une sortie par valeur | `sort -t$'\t' -k2 -nr out/part-r-00000 \| head` |
| Comparer deux résultats | `diff <(sort a.txt) <(sort b.txt) && echo IDENTIQUE` |
| Colonnes d'un CSV avec leur numéro | `head -1 f.csv \| tr ';' '\n' \| cat -n` |
| Hits d'une tranche | `grep -c ":10:\(3[0-9]\|4[0-4]\):" logs/cloudera_log.txt` |

Compteurs Hadoop (dans le log du job) qui prouvent la cohérence :

| Compteur | Doit valoir |
|---|---|
| `Map input records` | `wc -l` du fichier d'entrée |
| `Map output records` | lignes gardées par le filtre (× couples émis par ligne) |
| `Reduce input records` | = `Map output records` (si pas de combiner) |
| `Reduce input groups` | nombre de clés distinctes |
| `Reduce output records` | `wc -l part-r-00000` |
