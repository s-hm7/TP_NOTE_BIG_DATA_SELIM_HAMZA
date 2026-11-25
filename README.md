<!--
  CE FICHIER EST LE RAPPORT À RENDRE.
  - Les commandes et sorties exactes sont dans rapport/journal.md (rempli par j '...' et les scripts).
  - Pour chaque exercice : copier le bloc « Exercice N », coller les commandes/sorties du journal,
    écrire le choix clé/valeur et la vérification. Style : 1re personne, phrases simples.
  - Aide : antiseches/00_SOMMAIRE.md
-->

# TP noté — Hadoop / MapReduce / Spark

Sélim Hamza — Master MIAGE, Ingénierie des Processus Métier

## Environnement

Je travaille sur GitHub Codespaces. Le conteneur installe Java 8, Hadoop 3.2.1 et Spark 3.1.2
(`.devcontainer/setup.sh`). Hadoop tourne en mode local (`fs.defaultFS = file:///`) : ce que j'appelle
HDFS correspond au dossier `hdfs/` du dépôt. Je compile en Java 8.

```bash
$ ./scripts/check.sh
(coller la sortie)
```

Organisation : `data/` fichiers bruts (non versionnés, voir `data/README.md`) · `hdfs/` entrées et sorties
des jobs · un dossier par exercice MapReduce · `scala/` scripts spark-shell · `spark/` code DataFrames ·
`resultats/` sorties complètes · `rapport/journal.md` toutes les commandes lancées, avec leurs sorties.

---

## Exercice 1 — <titre>

### Ce qu'on me demande
Entrée : une ligne ressemble à `...`.
Sortie attendue : `...`.

### Mon choix
- Clé : `...` car je dois regrouper par ...
- Valeur : `...` car le Reducer doit ...
- Reducer : ...
- Filtre : j'ignore les lignes où ...

### Chargement des données
```bash
$ hadoop fs -mkdir -p hdfs/...
$ hadoop fs -put data/... hdfs/.../
$ hadoop fs -ls hdfs/...
```

### Compilation et lancement
```bash
(coller les lignes $ ... affichées par ./scripts/mr.sh)
```

### Résultat
```
(5 premières et 3 dernières lignes ; fichier complet : resultats/<exo>.txt)
```

### Vérification
Je vérifie avec une commande qui n'utilise pas mon programme :
```bash
$ ...
```
Les compteurs Hadoop confirment : `Map input records=...` (= `wc -l` du fichier).

### Problèmes rencontrés
- ...

---

## Exercice 2 — <titre>

(même plan)
