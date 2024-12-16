import json

def jsonl_to_json(input_file, output_file):
    """
    Převádí soubor JSONL na JSON.

    :param input_file: Cesta k vstupnímu souboru JSONL
    :param output_file: Cesta k výstupnímu souboru JSON
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            data = [json.loads(line) for line in infile]

        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(data, outfile, indent=4, ensure_ascii=False)

        print(f"Soubor byl úspěšně převeden: {output_file}")
    except Exception as e:
        print(f"Chyba při převodu souboru: {e}")

# Příklad použití
# Nahraďte 'input.jsonl' a 'output.json' názvy vašich souborů
input_file = '/home/user/skola/tpb/small.jsonl'
output_file = '/home/user/skola/tpb/kafka-stream/kafka-stream/idnes.json'

jsonl_to_json(input_file, output_file)
