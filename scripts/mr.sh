#!/usr/bin/env bash
# Usage : ./scripts/mr.sh <package> <entree> <sortie> [param ...]
# Ex.   : ./scripts/mr.sh hits hdfs/logs hdfs/out_hits
#         ./scripts/mr.sh index hdfs/sherlock hdfs/out_index gentleman
# Compile <package>/*.java -> <package>.jar -> supprime <sortie> -> hadoop jar -> compteurs + extrait.
# Chaque commande est affichée (préfixe $) : elles vont telles quelles dans le rapport.
set -u
if [ $# -lt 3 ]; then echo "Usage : $0 <package> <entree> <sortie> [param ...]"; exit 1; fi
pkg=$1; entree=$2; sortie=$3; shift 3
cd "$(dirname "$0")/.."

main=$(grep -l "static void main" "$pkg"/*.java 2>/dev/null | head -1)
if [ -z "$main" ]; then echo "Aucune classe avec main() dans $pkg/"; exit 1; fi
classe="$pkg.$(basename "$main" .java)"

run() { printf '$'; printf ' %q' "$@"; echo; "$@"; }
mkdir -p traces resultats

run hadoop fs -ls "$entree" || { echo "!! Entrée introuvable : $entree"; exit 1; }
run javac -cp "lib/*" -d . "$pkg"/*.java || { echo "!! Compilation échouée : rien n'est lancé"; exit 1; }
run jar cf "$pkg.jar" "$pkg"/*.class || exit 1
run rm -rf "$sortie"
printf '$ hadoop jar %s %s %s %s' "$pkg.jar" "$classe" "$entree" "$sortie"; [ $# -gt 0 ] && printf ' %q' "$@"; echo " > traces/$pkg.log 2>&1"
hadoop jar "$pkg.jar" "$classe" "$entree" "$sortie" "$@" > "traces/$pkg.log" 2>&1
rc=$?
echo "code retour : $rc"
if [ $rc -ne 0 ]; then
  echo "---- fin du log (traces/$pkg.log) ----"
  grep -E "Exception|Error|error:" "traces/$pkg.log" | head -5
  tail -15 "traces/$pkg.log"
  exit $rc
fi
echo "---- compteurs ----"
grep -E "Map input records|Map output records|Combine output records|Reduce input groups|Reduce output records" "traces/$pkg.log" | tail -5 | sed 's/^[[:space:]]*/  /'
nb=$(cat "$sortie"/part-* | wc -l)
echo "---- résultat : $nb ligne(s) dans $sortie ----"
cat "$sortie"/part-* | head -10
[ "$nb" -gt 10 ] && { echo "..."; cat "$sortie"/part-* | tail -3; }
cat "$sortie"/part-* > "resultats/$pkg.txt"
echo "(copie complète : resultats/$pkg.txt)"
