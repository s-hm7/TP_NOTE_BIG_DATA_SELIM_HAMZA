# 02 — HDFS : commandes shell

`hadoop fs -<cmd>` = `hdfs dfs -<cmd>` (mêmes options). Utilise **celle de l'énoncé**.
Aide : `hadoop fs` (liste) · `hadoop fs -help put`

## Les 3 systèmes de fichiers et leurs frontières

```
 PC Windows ──(glisser dans VS Code / docker cp)──► disque Linux ──(hadoop fs -put / -get)──► HDFS
```

| Environnement | Disque Linux | HDFS | Passer de l'un à l'autre |
|---|---|---|---|
| **Codespace (mode local)** | `/workspaces/<repo>` | **le même disque** | `-put` / `-get` copient de dossier à dossier |
| Docker du TP1 | bash du `namenode` | cluster | `docker cp f namenode:/` puis `hadoop fs -put` |

> En mode local, utilise des **chemins relatifs** (`logs`, `data/web`). Un chemin absolu comme `/user/root` vise la racine du disque → `Permission denied`.

## Commandes

| But | Commande | Note |
|---|---|---|
| Créer un dossier (et ses parents) | `hadoop fs -mkdir -p data/weblogs` | `-p` : pas d'erreur s'il existe |
| Lister | `hadoop fs -ls` · `hadoop fs -ls data` | |
| Lister récursivement | `hadoop fs -ls -R data` | |
| Envoyer disque → HDFS | `hadoop fs -put f.txt data/weblogs/` | `-put -f` pour écraser |
| Envoyer depuis l'entrée standard | `cat << 'EOF' \| hadoop fs -put - test/t.txt` | Créer un fichier de test **sans** fichier local |
| Rapatrier HDFS → disque | `hadoop fs -get data/web/log.txt .` | `-copyToLocal` = pareil |
| Copier dans HDFS | `hadoop fs -cp data/a.txt data/web/log.txt` | Peut renommer |
| Déplacer / renommer | `hadoop fs -mv a.txt b.txt` | |
| Afficher | `hadoop fs -cat out/part-r-00000` | |
| Début / fin | `hadoop fs -head f` · `hadoop fs -tail f` | `-tail` = **dernier Ko**, pas les N dernières lignes |
| N dernières lignes | `hadoop fs -cat f \| tail -5` | |
| Supprimer fichier / dossier | `hadoop fs -rm f` · `hadoop fs -rm -r dossier` | |
| Taille | `hadoop fs -du -h data` | |
| Compter lignes | `hadoop fs -cat f \| wc -l` | |
| Fusionner la sortie d'un job | `hadoop fs -getmerge out resultat.txt` | Concatène tous les `part-*` |
| Réplication (vrai cluster) | `hadoop fs -setrep -w 2 f` | Sans effet en mode local |
| Santé des blocs (vrai cluster) | `hdfs fsck data -files -blocks` | |
| Config active | `hdfs getconf -confKey fs.defaultFS` | |

**Fichiers ignorés par les jobs** : ceux qui commencent par `.` ou `_`. Les `.xxx.crc` (sommes de contrôle créées par `-put`) et `_SUCCESS` ne gênent donc pas.

**Le job lit TOUT le dossier d'entrée** : un vieux fichier de test oublié dedans fausse le résultat → `hadoop fs -ls <entrée>` avant chaque lancement.

## TP1 corrigé (version Codespace)

```bash
hadoop fs -mkdir -p user/root                       # 1. (/user/root sur un vrai cluster)
ls                                                  # 3. dossier courant (disque)
hadoop fs -ls                                       # 4. dossier courant (HDFS)
hadoop fs -mkdir -p data/weblogs                    # 5.
hadoop fs -put weblog_entries.txt data/weblogs/     # 6.
hadoop fs -ls data/weblogs                          # 7.
hadoop fs -mkdir -p data/web                        # 8.
hadoop fs -cp data/weblogs/weblog_entries.txt data/web/log.txt
hadoop fs -tail data/web/log.txt
hadoop fs -rm -r data/weblogs                       # 9.
hadoop fs -get data/web/log.txt .                   # 10.
```

## Version Docker du cours (si le sujet l'impose)

```powershell
docker-compose up -d                     # PowerShell, dans Docker-Hadoop
docker cp weblog_entries.txt namenode:/  # PC -> namenode
docker exec -it namenode bash            # entrer dans le namenode
```
```bash
hadoop fs -mkdir -p /user/root           # dans le namenode : chemins absolus OK
hadoop fs -put /weblog_entries.txt data/weblogs/
exit                                     # retour PowerShell
```
```powershell
docker cp namenode:/log.txt .            # namenode -> PC
docker-compose down                      # arrêter
```
Interface web : `http://localhost:9870` → Utilities → Browse the file system.

## Théorie express (questions de cours possibles)

| Notion | À retenir |
|---|---|
| Bloc | Fichier découpé en blocs (128 Mo par défaut en Hadoop 3) répartis sur les datanodes |
| Réplication | 3 copies par défaut, sur des machines (et racks) différents |
| Namenode | Garde les **métadonnées** (quel bloc est où), pas les données |
| Datanode | Stocke les blocs, envoie des heartbeats (3 s) et des rapports de blocs |
| Secondary namenode | Fusionne le journal des métadonnées ; **pas** un namenode de secours |
| Écriture | Le client demande au namenode, puis écrit en **pipeline** datanode → datanode |
| Lecture | Le namenode donne la liste des blocs ; le client lit sur le datanode le plus proche |
| Split MapReduce | ≈ 1 bloc = 1 Mapper. Au TP2, `cloudera_log.txt` (38 Mo) = **2 splits** de 32 Mo en local |
