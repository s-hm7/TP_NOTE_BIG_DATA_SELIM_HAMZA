# journal.sh — à charger avec :  source antiseches/journal.sh
# j  'commande'  : exécute la commande, l'affiche ET l'ajoute (avec sa sortie) à rapport/journal.md
# jn "texte"     : ajoute un titre ou une phrase au journal
# Attention : j lance un sous-shell -> un "cd" fait dans j ne reste pas. Fais les cd sans j.

JOURNAL="${JOURNAL:-rapport/journal.md}"

j() {
  mkdir -p "$(dirname "$JOURNAL")"
  printf '\n```bash\n$ %s\n' "$*" >> "$JOURNAL"
  bash -c "$*" 2>&1 | tee -a "$JOURNAL"
  local rc=${PIPESTATUS[0]}
  printf '```\n' >> "$JOURNAL"
  [ "$rc" -ne 0 ] && printf '> code retour : %s\n' "$rc" >> "$JOURNAL"
  return $rc
}

jn() {
  mkdir -p "$(dirname "$JOURNAL")"
  printf '\n%s\n' "$*" >> "$JOURNAL"
}

echo "Journal actif -> $JOURNAL   (j 'commande' | jn \"texte\")"
