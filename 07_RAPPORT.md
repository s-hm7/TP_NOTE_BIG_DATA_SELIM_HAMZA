# 07 — Rapport : toutes les commandes, sans effort

Le rapport doit contenir **toutes les commandes de chaque étape**. Ne les recopie pas à la main : fais-les enregistrer pendant que tu travailles.

## 1. Le journal automatique (`journal.sh`)

```bash
source antiseches/journal.sh          # à refaire dans chaque nouveau terminal
jn "## Exercice 1 — Hits par heure"    # titre dans le journal
j 'hadoop fs -mkdir -p logs'             # exécute + enregistre commande ET sortie
j 'hadoop fs -put cloudera_log.txt logs/'
j 'hadoop fs -ls logs'
```

Résultat dans `rapport/journal.md` :

````markdown
## Exercice 1 — Hits par heure

```bash
$ hadoop fs -ls logs
Found 1 items
-rw-r--r--   1 codespace codespace   39773868 2026-11-23 15:33 logs/cloudera_log.txt
```
````

| Règle | Pourquoi |
|---|---|
| Mettre la commande entre **apostrophes** `'...'` | Les `\|`, `>`, `&&` sont gardés dans la commande |
| Faire les `cd` **sans** `j` | `j` tourne dans un sous-shell : le `cd` ne resterait pas |
| Pour `hadoop jar` : rediriger le log | Sinon 200 lignes de log dans le rapport |
| Un échec est noté `> code retour : N` | Les erreurs corrigées valorisent le rapport : garde-les |

### Lancer un job proprement dans le journal
```bash
mkdir -p traces
j 'javac -cp "lib/*" -d . hits/HitsDriver.java && jar cvf hits.jar hits/*.class'
j 'rm -rf output_hits; hadoop jar hits.jar hits.HitsDriver logs output_hits > traces/hits.log 2>&1; echo "code retour: $?"'
j 'grep -A 25 "Counters:" traces/hits.log | grep -E "input records|output records|groups"'
j 'head -5 output_hits/part-r-00000; echo ...; tail -3 output_hits/part-r-00000'
j 'grep -c ":10:\(3[0-9]\|4[0-4]\):" logs/cloudera_log.txt'      # vérification indépendante
```

### Spark-shell (interactif) : capturer la session
```bash
script -q -a rapport/spark_session.log     # enregistre tout ce qui s'affiche
spark-shell
# ... :paste scala/hits.scala ...   puis :quit
exit                                       # arrête l'enregistrement
```
Puis copier les passages utiles dans le rapport. Mieux encore : garder le code dans `scala/*.scala` et coller seulement les sorties.

### Filet de sécurité
```bash
history > rapport/historique_commandes.txt     # toutes les commandes tapées dans ce terminal
```

## 2. Structure du dépôt à rendre

```
<repo>/
├── README.md                 ← le rapport final (ou rapport/RAPPORT.md)
├── rapport/
│   └── journal.md            ← journal brut (commandes + sorties)
├── antiseches/               ← ces fiches + journal.sh (à retirer si interdit dans le rendu)
├── lib/                      ← jars Hadoop
├── exo1/Exo1Driver.java      ← 1 dossier = 1 package = 1 exercice MapReduce
├── scala/exo3.scala          ← scripts spark-shell
├── spark/AppSpark.java       ← DataFrames
├── resultats/                ← copies des part-r-00000 (petits)
└── .gitignore
```

`.gitignore` :
```
*.class
*.jar
output_*/
traces/
data/*.csv
*.tgz
```
Les données (38 Mo de logs, 36 Mo d'arbres) restent **hors du dépôt** : le rapport indique leur nom et leur source.
Garde les résultats : `mkdir -p resultats && cp output_hits/part-r-00000 resultats/hits.txt`.

## 3. Modèle du rapport (style étudiant, à la 1re personne)

````markdown
# TP noté — Hadoop / MapReduce / Spark
Sélim Hamza — M1 MIAGE IPM

## Environnement
Je travaille sur GitHub Codespaces. Hadoop 3.2.1 tourne en mode local (`fs.defaultFS = file:///`),
donc HDFS correspond au disque du Codespace. Je compile en Java 8.

```bash
$ java -version
openjdk version "1.8.0_..."
$ hadoop version
Hadoop 3.2.1
```

## Exercice N — <titre de l'énoncé>

### Ce qu'on me demande
Entrée : une ligne ressemble à `...`. Sortie attendue : `...`.

### Mon choix de clé et de valeur
- Clé : `...` parce que je dois regrouper par ...
- Valeur : `...` parce que le Reducer doit ...
- Reducer : ...
- Filtre : j'ignore les lignes où ...

### Chargement des données
```bash
$ hadoop fs -mkdir -p ...
$ hadoop fs -put ... ...
$ hadoop fs -ls ...
```

### Compilation et lancement
```bash
$ javac -cp "lib/*" -d . exoN/ExoNDriver.java && jar cvf exoN.jar exoN/*.class
$ hadoop jar exoN.jar exoN.ExoNDriver ... ...
```

### Résultat
```
(extrait de part-r-00000 : 5 premières et 3 dernières lignes)
```

### Vérification
Je vérifie avec une commande qui n'utilise pas mon programme :
```bash
$ grep -c ... ...
2486
```
Le résultat est le même que la ligne `10:30 2486` de mon job. Les compteurs Hadoop confirment :
`Map input records=180000` (= `wc -l` du fichier).

### Problèmes rencontrés
- J'ai eu `FileAlreadyExistsException` car le dossier de sortie existait : je le supprime avant chaque lancement.
- ...
````

## 4. Ce qui fait la différence à la correction

| Faire | Éviter |
|---|---|
| Expliquer **pourquoi** cette clé et cette valeur | Coller le code sans un mot |
| Montrer une **vérification indépendante** (grep, awk, compteurs) | Affirmer « ça marche » |
| Garder les erreurs rencontrées et leur correction | Cacher les échecs |
| Extraits de sortie (début / fin) | 500 lignes de `part-r-00000` |
| Signaler une ambiguïté de l'énoncé et le choix fait (ex. borne 23h15/23h30) | Choisir en silence |
| Commandes **exactes** qui ont tourné (journal) | Commandes réécrites de mémoire |
