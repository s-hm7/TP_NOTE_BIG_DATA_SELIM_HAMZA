# 03 — MapReduce en Java

## 1. La chaîne complète (à copier telle quelle)

```bash
# exo = nom du dossier = nom du package ; ExoDriver = classe avec le main
hadoop fs -ls entree                                                   # vérifier l'entrée
javac -cp "lib/*" -d . exo/ExoDriver.java \
 && jar cvf exo.jar exo/*.class > /dev/null \
 && rm -rf sortie \
 && hadoop jar exo.jar exo.ExoDriver entree sortie > traces/exo.log 2>&1 ; echo "code retour: $?"
hadoop fs -ls sortie                                                   # _SUCCESS + part-r-00000
hadoop fs -cat sortie/part-r-00000 | head -20
grep -A 25 "Counters:" traces/exo.log | grep -E "input records|output records|groups"
```

| Morceau | Rôle |
|---|---|
| `-cp "lib/*"` | Les classes Hadoop pour compiler. **Guillemets obligatoires** |
| `-d .` | Range les `.class` dans `exo/` (le dossier du package) |
| `jar cvf exo.jar exo/*.class` | Le jar doit contenir `exo/ExoDriver.class` (et les `$Mapper`, `$Reducer`) |
| `&&` | La suite ne s'exécute que si l'étape précédente a réussi |
| `rm -rf sortie` | Hadoop refuse d'écrire dans un dossier existant |
| `exo.ExoDriver` | `package.Classe`, **pas** le chemin du fichier |
| `> traces/exo.log 2>&1` | Les 200 lignes de log vont dans un fichier (`mkdir -p traces` avant) |
| `code retour: 0` | Succès. Sinon : `tail -30 traces/exo.log` |
| ⚠️ `traces/` hors du dossier d'entrée | Un log rangé dans le dossier d'entrée serait relu comme une donnée au job suivant |

Compilé avec un Java récent ? Ajoute `--release 8` : `javac --release 8 -cp "lib/*" -d . ...`

## 2. Le schéma à avoir en tête

```
ligne ─► MAP ─► (clé, valeur) ─► SHUFFLE (tri + regroupement par clé) ─► REDUCE ─► (clé, résultat)
1 appel/ligne    0, 1 ou n couples      fait par Hadoop, gratuit              1 appel/clé
```

**Toute la logique = choisir la clé.** Hadoop regroupe automatiquement tout ce qui a la même clé.

| Je veux… | Clé émise par le Map | Valeur émise | Reduce |
|---|---|---|---|
| compter par X | X | `1` | somme |
| lister les éléments par X | X | l'élément | `TreeSet` → liste |
| compter les Y **différents** par X | X | Y | `HashSet.size()` |
| moyenne / max / min de V par X | X | V | calcul |
| un total global | une constante (`"total"`) | V | somme |

## 3. Les types : la règle qui évite 50 % des erreurs

```java
Mapper <LongWritable, Text,  K_MAP, V_MAP>   // entrée fixe pour un fichier texte : offset, ligne
Reducer<K_MAP,        V_MAP, K_RED, V_RED>   // entrée du Reducer = sortie du Mapper, À L'IDENTIQUE
```

| Java | Hadoop | Lire / créer |
|---|---|---|
| `String` | `Text` | `t.toString()` / `new Text(s)` |
| `int` | `IntWritable` | `w.get()` / `new IntWritable(n)` |
| `long` | `LongWritable` | `w.get()` |
| `double` | `DoubleWritable` | `w.get()` / `new DoubleWritable(d)` |
| rien | `NullWritable` | `NullWritable.get()` |

Driver :
- sorties Map et Reduce **identiques** → `setOutputKeyClass` + `setOutputValueClass` suffisent ;
- sorties **différentes** → ajouter `setMapOutputKeyClass` + `setMapOutputValueClass`, sinon erreur `Type mismatch in value from map` au shuffle.

## 4. Le modèle générique (point de départ de TOUT exercice)

Un seul fichier, 3 classes. Copier dans `<exo>/<Exo>Driver.java`, puis remplacer `exo`/`Exo` et les `TODO`.

Créer un nouvel exercice (ici `top`) en une commande, à partir du fichier `antiseches/modeles/ExoDriver.java` :
```bash
mkdir -p top && sed 's/^package exo;/package top;/; s/Exo/Top/g' antiseches/modeles/ExoDriver.java > top/TopDriver.java
```

```java
package exo;                                    // = nom du dossier (exo/ExoDriver.java)

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExoDriver {

    // ================= MAPPER : appelé 1 fois par LIGNE, émet 0, 1 ou n couples (clé, valeur)
    //                 entrée (ne pas toucher)  | sortie Map : <K_MAP, V_MAP>
    public static class ExoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private String param;                                     // 3e argument de la commande (ou null)

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            param = context.getConfiguration().get("param");      // lu 1 fois, pas à chaque ligne
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String ligne = value.toString().trim();               // trim() enlève aussi le \r Windows
            if (ligne.isEmpty()) return;                          // ligne vide : ignorée
            // if (key.get() == 0) return;                        // CSV avec en-tête : saute la 1re ligne

            String[] champs = ligne.split(";");                   // ";" CSV | "\\s+" mots | "\t" après cat -n
            // if (champs.length < 3) return;                     // ligne incomplète : ignorée

            // TODO 1 — FILTRE : return si la ligne ne m'intéresse pas
            // TODO 2 — CLÉ = ce sur quoi on regroupe ; VALEUR = ce qu'on agrège
            String cle = champs[0];
            context.write(new Text(cle), new IntWritable(1));
        }
    }

    // ================= REDUCER : appelé 1 fois par CLÉ, avec TOUTES ses valeurs
    //                   entrée = sortie Map         | sortie finale : <K_RED, V_RED>
    public static class ExoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int somme = 0;
            for (IntWritable v : values) {
                somme += v.get();                                 // v.get() : IntWritable -> int
            }
            // TODO 3 — filtre sur le résultat (ex. if (somme < 2) return;)
            context.write(key, new IntWritable(somme));
        }
    }

    // ================= DRIVER : configure et lance le job
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage : ExoDriver <rep_entree> <rep_sortie> [param]");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        if (args.length > 2) job.getConfiguration().set("param", args[2]);   // transmis au Mapper

        job.setJarByClass(ExoDriver.class);
        job.setJobName("Exo");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));          // ne doit PAS exister

        job.setMapperClass(ExoMapper.class);
        job.setReducerClass(ExoReducer.class);
        // job.setCombinerClass(ExoReducer.class);  // si somme/max ET sortie Reducer = sortie Map
        // job.setNumReduceTasks(1);                 // Top-N global : un seul Reducer
        // job.setNumReduceTasks(0);                 // map-only (filtre) : sortie part-m-00000

        job.setMapOutputKeyClass(Text.class);           // = K_MAP
        job.setMapOutputValueClass(IntWritable.class);  // = V_MAP
        job.setOutputKeyClass(Text.class);              // = K_RED
        job.setOutputValueClass(IntWritable.class);     // = V_RED

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Adapter le modèle en 5 décisions

| # | Décision | Où |
|---|---|---|
| 1 | Comment découper la ligne (`split(";")`, `split("\\s+")`, `split("\t", 2)`, `split(" ")` puis `split(":")`) | Mapper |
| 2 | Quelles lignes ignorer (en-tête, ligne courte, hors plage) → `return` | Mapper |
| 3 | **Clé** et **valeur** (types Hadoop) | Mapper + génériques |
| 4 | Agrégat : somme, `TreeSet`, `HashSet`, moyenne, max | Reducer |
| 5 | Mettre à jour les génériques **et** les 4 `set...Class` du Driver | Driver |

Paramètre en ligne de commande : `hadoop jar exo.jar exo.ExoDriver entree sortie burglar` → `param` vaut `"burglar"` dans le Mapper.

## 5. Recettes (compilées et testées)

Chaque recette remplace le Mapper et le Reducer du modèle. Les lignes `Driver` indiquent les types à déclarer.

### 5.1 Moyenne (en écartant les zéros)
`Driver : MapOutput (Text, DoubleWritable) · Output (Text, DoubleWritable) · pas de combiner`

```java
// Entrée : produit;prix   ->   produit  moyenne
public static class M extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] c = value.toString().trim().split(";");
        if (c.length < 2) return;
        try {
            double x = Double.parseDouble(c[1].trim().replace(',', '.'));   // "12,5" -> 12.5
            if (x <= 0) return;                                             // mesures à 0 écartées
            context.write(new Text(c[0].trim()), new DoubleWritable(x));
        } catch (NumberFormatException e) { /* en-tête ou valeur vide : ignorée */ }
    }
}
public static class R extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double somme = 0; int n = 0;
        for (DoubleWritable v : values) { somme += v.get(); n++; }
        context.write(key, new DoubleWritable(somme / n));
    }
}
```
**Pas de combiner pour une moyenne** : la moyenne de moyennes partielles est fausse. (Pour en mettre un, émettre `"somme;n"` en `Text`.)

### 5.2 Maximum (ou minimum)
`Driver : (Text, IntWritable) partout · combiner possible : setCombinerClass(R.class)`

```java
// Entrée : ville;temperature   ->   ville  max
public static class M extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] c = value.toString().trim().split(";");
        if (c.length < 2 || !c[1].trim().matches("-?\\d+")) return;        // en-tête / valeur non entière
        context.write(new Text(c[0].trim()), new IntWritable(Integer.parseInt(c[1].trim())));
    }
}
public static class R extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        for (IntWritable v : values) max = Math.max(max, v.get());
        context.write(key, new IntWritable(max));
    }
}
```

### 5.3 Compter des éléments DIFFÉRENTS
`Driver : MapOutput (Text, Text) · Output (Text, IntWritable)`

```java
// Entrée : id_cde;id_client;produit   ->   produit  nb de clients DIFFÉRENTS
public static class M extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] c = value.toString().trim().split(";");
        if (c.length < 3) return;
        context.write(new Text(c[2].trim()), new Text(c[1].trim()));       // (ce qu'on compte, qui on compte)
    }
}
public static class R extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> distincts = new HashSet<String>();
        for (Text v : values) distincts.add(v.toString());
        context.write(key, new IntWritable(distincts.size()));
    }
}
```

### 5.4 Regrouper et lister (famille des anagrammes)
`Driver : (Text, Text) partout`

```java
// Regrouper des éléments sous une clé calculée et les lister (anagrammes, mots par initiale, ...)
public static class M extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for (String w : value.toString().toLowerCase().split("\\W+"))
            if (w.length() > 0) context.write(new Text(w.substring(0, 1)), new Text(w));   // clé = initiale
    }
}
public static class R extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<String> set = new TreeSet<String>();
        for (Text v : values) set.add(v.toString());
        if (set.size() < 2) return;
        context.write(key, new Text(set.size() + " : " + String.join(" ", set)));
    }
}
```

### 5.5 Top N (les N plus fréquents)
`Driver : (Text, IntWritable) partout · job.setNumReduceTasks(1) · N : job.getConfiguration().set("n", args[2])`

```java
// Mapper = celui du comptage (mot, 1). Reducer : garde tout, trie et écrit les N premiers À LA FIN.
public static class M extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for (String w : value.toString().toLowerCase().split("\\W+"))
            if (w.length() > 0) context.write(new Text(w), new IntWritable(1));
    }
}
public static class R extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final List<String[]> tous = new ArrayList<String[]>();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int s = 0;
        for (IntWritable v : values) s += v.get();
        tous.add(new String[]{ key.toString(), String.valueOf(s) });       // key.toString() : Text réutilisé
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String p = context.getConfiguration().get("n");
        int n = (p == null) ? 10 : Integer.parseInt(p);
        tous.sort((a, b) -> Integer.parseInt(b[1]) - Integer.parseInt(a[1]));   // décroissant
        for (int i = 0; i < Math.min(n, tous.size()); i++)
            context.write(new Text(tous.get(i)[0]), new IntWritable(Integer.parseInt(tous.get(i)[1])));
    }
}
```
**Pourquoi `cleanup`** : `reduce` voit une clé à la fois. `cleanup` est appelé **une fois à la fin**, quand toutes les clés sont connues. Un seul Reducer, sinon tu obtiens un top par Reducer.

Alternative sans code, pour le rapport : `sort -t$'\t' -k2 -nr sortie/part-r-00000 | head -10`

### 5.6 Jointure de 2 fichiers (côté Reduce)
`Driver : MapOutput (Text, Text) · Output (Text, IntWritable) · les 2 fichiers dans le MÊME dossier d'entrée`

```java
// Dossier d'entrée avec 2 fichiers : clients.csv (id_client;nom) et commandes.csv (id_cde;id_client;...)
// Résultat : nom  nombre de commandes
public static class M extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fichier = ((FileSplit) context.getInputSplit()).getPath().getName();   // d'où vient la ligne ?
        String[] c = value.toString().trim().split(";");
        if (c.length < 2) return;
        if (fichier.startsWith("clients")) context.write(new Text(c[0].trim()), new Text("C;" + c[1].trim()));
        else                               context.write(new Text(c[1].trim()), new Text("O;1"));
    }
}
public static class R extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String nom = "inconnu(" + key + ")"; int n = 0;
        for (Text v : values) {
            String s = v.toString();
            if (s.startsWith("C;")) nom = s.substring(2); else n++;
        }
        context.write(new Text(nom), new IntWritable(n));
    }
}
```
**Principe** : les deux fichiers émettent la **même clé** (l'id commun) avec une étiquette (`C;` ou `O;`) ; le Reducer reçoit les deux côtés ensemble.
Import en plus : `org.apache.hadoop.mapreduce.lib.input.FileSplit`.

### 5.7 Filtre seul (map-only)
`Driver : job.setNumReduceTasks(0) · Output (Text, NullWritable) · résultat dans part-m-00000`

```java
// Map-only : garder les lignes qui contiennent "404". Driver : job.setNumReduceTasks(0);
// + setOutputKeyClass(Text.class) ; setOutputValueClass(NullWritable.class)
public static class M extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().contains("\" 404 ")) context.write(value, NullWritable.get());
    }
}
```

## 6. TP2 : les 4 solutions finales (testées) et leurs résultats réels

### Exo 1 — WordCount (code du prof)
```bash
hadoop fs -mkdir -p gutenberg && hadoop fs -put Shakespeare.txt gutenberg/
hadoop jar wc.jar wc.WcDriver gutenberg wordcounts
```
`split("\\W+")` coupe sur tout ce qui n'est pas `[a-zA-Z0-9_]` → les mots accentués sont coupés (`é` = séparateur). Pour du français : `split("[^\\p{L}]+")`.

### Exo 2 — Anagrammes · clé = lettres triées **sans accent**, valeur = mot **avec accent**
```java
package anagramme;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.TreeSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramDriver {

    // (lettres triées SANS accent, mot AVEC accent)
    public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            for (String mot : value.toString().split("\\s+")) {
                mot = mot.trim().toLowerCase().replaceAll("[^a-zàâäéèêëîïôöùûüç]", "");
                if (mot.length() > 0) {
                    String sansAccent = Normalizer.normalize(mot, Normalizer.Form.NFD)
                                                  .replaceAll("\\p{M}", "");
                    char[] lettres = sansAccent.toCharArray();
                    Arrays.sort(lettres);
                    context.write(new Text(new String(lettres)), new Text(mot));
                }
            }
        }
    }

    // mots DISTINCTS d'une clé, seulement si au moins 2
    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeSet<String> mots = new TreeSet<String>();
            for (Text val : values) mots.add(val.toString());
            if (mots.size() > 1) context.write(key, new Text(": " + String.join(" ", mots)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) { System.out.println("Usage: AnagramDriver <in> <out>"); System.exit(-1); }
        Job job = Job.getInstance();
        job.setJarByClass(AnagramDriver.class);
        job.setJobName("Anagram Finder");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);       // Map et Reduce sortent (Text, Text) : 2 lignes suffisent
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
```bash
hadoop fs -mkdir -p mots && hadoop fs -put liste_mots.txt mots/
hadoop jar anagramme.jar anagramme.AnagramDriver mots output_anagrammes
```
**Résultat réel : 17 groupes** (le corrigé du PDF en montre 11, tous inclus dans les miens ; les 6 autres sont de vrais anagrammes, ex. `aube beau`).
Ex. `aegnrt : argent grenat gréant gérant régnât tanger`.
Erreurs rencontrées : `é` trié comme une lettre à part (3 groupes au lieu d'1) → `Normalizer` ; mots répétés et `takimata ×300` compté comme groupe → `TreeSet` + `size() > 1`.

### Exo 3 — Hits par quart d'heure · clé = `HH:MM` du début de tranche, valeur = 1
```java
package hits;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HitsDriver {

    // (tranche de 15 min, 1)
    public static class HitsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable UN = new IntWritable(1);
        private static final int DEBUT = 10 * 60 + 30;   // 10h30
        private static final int FIN   = 23 * 60 + 30;   // dernière tranche affichée par le corrigé : 23:30

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 79.133.215.123 - - [14/Jun/2014:10:30:13 -0400] "GET /home ..."
            String[] champs = value.toString().split(" ");
            if (champs.length < 4) return;
            String[] t = champs[3].split(":");               // ["[14/Jun/2014","10","30","13"]
            if (t.length < 3) return;
            int h = Integer.parseInt(t[1]);
            int min = Integer.parseInt(t[2]);
            int quart = min / 15 * 15;                       // 44 -> 30 ; 45 -> 45
            int total = h * 60 + quart;
            if (total < DEBUT || total > FIN) return;
            context.write(new Text(String.format("%02d:%02d", h, quart)), UN);
        }
    }

    public static class HitsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int somme = 0;
            for (IntWritable v : values) somme += v.get();
            context.write(key, new IntWritable(somme));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) { System.out.println("Usage: HitsDriver <in> <out>"); System.exit(-1); }
        Job job = Job.getInstance();
        job.setJarByClass(HitsDriver.class);
        job.setJobName("Hits par quart d'heure");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(HitsMapper.class);
        job.setCombinerClass(HitsReducer.class);   // somme partielle côté Map : moins de données au shuffle
        job.setReducerClass(HitsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
**Résultat réel : 53 tranches**, de `10:30 2486` à `23:30 7243`, identique au corrigé (qui écrit `11:0`, moi `11:00`).
Contrôle indépendant : `grep -c ":10:\(3[0-9]\|4[0-4]\):" logs/cloudera_log.txt` → `2486`.
Compteurs : `Map input records=180000`, `Reduce output records=53`, `number of splits:2`.
Le **combiner** est valable ici (somme, mêmes types en entrée et sortie).

### Exo 4 — Index d'un mot · fichier préparé par `cat -n`, paramètre `mot_a_indexer`
```bash
cat -n The_Adventures_of_Sherlock_Holmes.txt > sherlock_num.txt     # "    42<TAB>texte"
hadoop fs -mkdir -p sherlock && hadoop fs -put sherlock_num.txt sherlock/
hadoop jar index.jar index.IndexDriver sherlock output_index gentleman
```
```java
package index;

import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexDriver {

    // fichier préparé par cat -n : "    42<TAB>texte"  ->  (mot, 42)
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String mot = context.getConfiguration().get("mot_a_indexer");
            String[] parties = value.toString().split("\t", 2);
            if (parties.length < 2) return;
            int numero = Integer.parseInt(parties[0].trim());
            for (String w : parties[1].split("\\W+")) {
                if (w.equalsIgnoreCase(mot)) {
                    context.write(new Text(mot), new IntWritable(numero));
                    break;                                   // 1 entrée par ligne
                }
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            TreeSet<Integer> lignes = new TreeSet<Integer>();   // tri numérique + sans doublon
            for (IntWritable v : values) lignes.add(v.get());
            context.write(key, new Text(lignes.size() + " " + lignes.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) { System.out.println("Usage: IndexDriver <in> <out> <mot>"); System.exit(-1); }
        Job job = Job.getInstance();
        job.getConfiguration().set("mot_a_indexer", args[2]);
        job.setJarByClass(IndexDriver.class);
        job.setJobName("Index d'un mot");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);          // Map : (Text, IntWritable)
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);             // Reduce : (Text, Text)  -> 4 lignes obligatoires
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
**Résultats** : `gentleman 45` (vérifié en TP) ; attendus par l'énoncé : `burglar 1`, `murderer 5`, `bachelor 8`, `innocent 17`.
Pourquoi `cat -n` : la clé reçue par `map` est l'**offset en octets**, pas le numéro de ligne.
Boucle de contrôle :
```bash
for m in burglar murderer bachelor gentleman innocent; do
  rm -rf output_index
  hadoop jar index.jar index.IndexDriver sherlock output_index $m > /dev/null 2>&1
  echo -n "$m : "; cut -f2 output_index/part-r-00000 | cut -d' ' -f1
done
```

### Exo 5 — Couples de produits · clé = `"x,y"` avec x<y, valeur = client
```java
package paires;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairesDriver {

    // id_cde;id_client;p1;p2;...  ->  ("x,y", client) pour chaque couple x<y
    public static class PairesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] champs = value.toString().trim().split(";");
            if (champs.length < 4) return;                    // il faut au moins 2 produits
            String client = champs[1].trim();
            TreeSet<String> set = new TreeSet<String>();      // dédoublonne ET trie les produits
            for (int k = 2; k < champs.length; k++) set.add(champs[k].trim());
            String[] prods = set.toArray(new String[0]);
            for (int i = 0; i < prods.length; i++)
                for (int j = i + 1; j < prods.length; j++)    // j > i : ni (p,p) ni (y,x)
                    context.write(new Text(prods[i] + "," + prods[j]), new Text(client));
        }
    }

    // nombre de clients DIFFÉRENTS par couple
    public static class PairesReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashSet<String> clients = new HashSet<String>();
            for (Text v : values) clients.add(v.toString());
            context.write(key, new IntWritable(clients.size()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) { System.out.println("Usage: PairesDriver <in> <out>"); System.exit(-1); }
        Job job = Job.getInstance();
        job.setJarByClass(PairesDriver.class);
        job.setJobName("Couples de produits");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PairesMapper.class);
        job.setReducerClass(PairesReducer.class);        // PAS de combiner : il compterait des clients partiels
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
Données de test générées (5004 commandes) + référence `attendu.txt` (190 couples, `p01,p02 → 112`) calculée en Python :
```bash
hadoop fs -mkdir -p commandes && hadoop fs -put -f commandes.txt commandes/
hadoop jar paires.jar paires.PairesDriver commandes output_paires
tr -s ' ' '\t' < output_paires/part-r-00000 | diff - attendu.txt && echo IDENTIQUE
```
Une commande de n produits émet n(n−1)/2 couples (20 produits → 190) : c'est le job le plus coûteux.

## 7. Pièges propres au code Java

| Piège | Conséquence | Bon réflexe |
|---|---|---|
| `set.add(val)` avec `val` de type `Text` | Toutes les entrées = la dernière valeur (Hadoop réutilise l'objet) | `val.toString()` / `v.get()` |
| `a == b` sur des `String` | Toujours faux | `a.equals(b)` / `equalsIgnoreCase` |
| `split("\\W+")` sur du français | `régnât` → `r`, `gn`, `t` | `split("[^\\p{L}]+")` |
| Premier élément vide de `split` | `" a b".split("\\s+")` → `["", "a", "b"]` | `if (w.length() > 0)` ou `trim()` avant |
| `Integer.parseInt(" 42")` | `NumberFormatException` | `.trim()` |
| Combiner sur une moyenne ou un comptage distinct | Résultat faux, sans erreur | Combiner seulement pour somme, max, min |
| Tri des clés `Text` | Ordre alphabétique : `10` avant `9` | `String.format("%02d", n)` ou `TreeSet<Integer>` dans le Reducer |
| Oubli de `@Override` + mauvaise signature | Ta méthode n'est jamais appelée (map identité) | Garder `@Override` : `javac` signale l'erreur |
