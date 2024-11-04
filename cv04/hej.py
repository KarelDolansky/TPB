# Definujte cestu k původnímu a novému souboru
input_file = "scraped_articles.jsonl"  # Nahraďte názvem vašeho JSONL souboru
output_file = "small.jsonl"

# Nastavte požadovaný počet záznamů
max_records = 20000

# Načtěte prvních 20000 řádků a uložte je do nového souboru
with open(input_file, "r") as infile, open(output_file, "w") as outfile:
    for i, line in enumerate(infile):
        if i < max_records:
            outfile.write(line)
        else:
            break

print(f"Zmenšený soubor s {max_records} záznamy byl uložen jako '{output_file}'")

