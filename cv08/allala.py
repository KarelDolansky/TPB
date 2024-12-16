max_values = {}

# Postupné čtení souboru
with open("output.log", "r") as file:
    for line in file:
        # Vytažení písmene a čísla
        line = line.strip().strip("()")
        letter, number = line.split(",")
        letter = letter.strip()
        number = int(number.strip())

        # Porovnání a uložení maximální hodnoty
        if letter not in max_values or number > max_values[letter]:
            max_values[letter] = number

# Výstup výsledků
for letter, max_number in sorted(max_values.items()):
    print(f"({letter},{max_number})")
