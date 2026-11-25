#!/usr/bin/env bash
# Usage : ./scripts/nouveau.sh mr <nom>      -> <nom>/<Nom>Driver.java   (modèle MapReduce générique)
#         ./scripts/nouveau.sh scala <nom>   -> scala/<nom>.scala         (modèle spark-shell)
# <nom> : minuscules, sans espace ni tiret (devient le nom du package Java). Ex. : top, hitsheure
set -u
cd "$(dirname "$0")/.."
if [ $# -ne 2 ]; then sed -n '2,4p' "$0"; exit 1; fi
type=$1; nom=$2
if ! [[ "$nom" =~ ^[a-z][a-z0-9_]*$ ]]; then echo "Nom invalide : '$nom' (minuscules, chiffres, _)"; exit 1; fi
Nom="${nom^}"
case "$type" in
  mr)
    [ -e "$nom" ] && { echo "$nom/ existe déjà"; exit 1; }
    mkdir -p "$nom"
    sed "s/^package exo;/package $nom;/; s/Exo/$Nom/g; s#(exo/#($nom/#" modeles/ExoDriver.java.txt > "$nom/${Nom}Driver.java"
    echo "Créé : $nom/${Nom}Driver.java"
    echo "Lancer : ./scripts/mr.sh $nom hdfs/<entree> hdfs/out_$nom [param]" ;;
  scala)
    f="scala/$nom.scala"
    [ -e "$f" ] && { echo "$f existe déjà"; exit 1; }
    sed "s/<nom>/$nom/g" modeles/exo.scala.txt > "$f"
    echo "Créé : $f"
    echo "Lancer : spark-shell   puis   :paste $f" ;;
  *) sed -n '2,4p' "$0"; exit 1 ;;
esac
