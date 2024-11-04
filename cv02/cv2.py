import re
from collections import Counter
from datetime import datetime
import json

# Funkce pro extrakci počtu komentářů z textu
def extract_comment_count(comment_text):
    match = re.search(r'\((\d+)\s+příspěvků\)', comment_text)
    return int(match.group(1)) if match else 0

# pocet clanku
def count_articles(articles):
    pocet_clanku = len(articles)
    return f'Pocet clanku: {pocet_clanku}'

# pocet duplicitnich clanku a jaký je to článek
def count_duplicate_articles(articles):
    article_keys = [(article['title'], article['content']) for article in articles]
    counter = Counter(article_keys)
    duplicate_keys = [key for key, count in counter.items() if count > 1]
    pocet_duplicitnich_clanku = len(duplicate_keys)
    return f'Počet duplicitních článků: {pocet_duplicitnich_clanku}'


# vypiste datum nejstarsiho clanku
def oldest_article_date(articles):
    date_formats = ['%Y-%m-%d', '%d. %m. %Y %H:%M']
    dates = []
    for article in articles:
        for date_format in date_formats:
            try:
                dates.append(datetime.strptime(article['publication_date'], date_format))
                break
            except ValueError:
                continue
    nejstarsi_datum = min(dates).strftime('%Y-%m-%d')
    return f'Nejstarší datum článku: {nejstarsi_datum}'



# Získání názvu článku s nejvíce komentáři
def most_commented_article(articles):
    article_comments = {article['title']: extract_comment_count(article['comment_count']) for article in articles}
    max_comments_article = max(article_comments, key=article_comments.get)
    return f'Článek s nejvíce komentáři: {max_comments_article}'

# vypiste nejvetsi pocet pridany fotek u clanku
def most_images(articles):
    images = {article['title']: article['image_count'] for article in articles}
    max_images = max(images, key=images.get)
    return f'Článek s nejvíce obrázky: {max_images}'
# vypiste pocty clanku podle roku publikace
def articles_by_year(articles):
    years = []
    date_formats = ['%Y-%m-%d', '%d. %m. %Y %H:%M']
    for article in articles:
        for date_format in date_formats:
            try:
                years.append(datetime.strptime(article['publication_date'], date_format).year)
                break
            except ValueError:
                continue
    article_counts = Counter(years)
    return '\n'.join([f'Rok {year}: {count} článků' for year, count in article_counts.items()])
# vypiste pocet unikatních kategogii a pocet clanku v nich
def unique_categories(articles):
    categories = Counter([article['category'] for article in articles])
    return f'Počet unikátních kategorií: {len(categories)}\n' + '\n'.join([f'{category}: {count} článků' for category, count in categories.items()])

# vypiste celkovy pocet komentaru
def total_comments(articles):
    return f'Celkový počet komentářů: {sum([extract_comment_count(article["comment_count"]) for article in articles])}'

# vypiste celkovy pocet slov ve vsech clancich
def total_words(articles):
    return f'Celkový počet slov: {sum([len(re.findall(r"\b\w+\b", article['content'])) for article in articles])}'


def process_words(articles, min_length=6, year=None):
    words = Counter()
    date_formats = ['%Y-%m-%d', '%d. %m. %Y %H:%M']

    if year:
        for article in articles:
            for date_format in date_formats:
                try:
                    date = datetime.strptime(article['publication_date'], date_format)
                    if date.year == year:
                        words.update([word for word in re.findall(r'\b\w+\b', article['content'].lower()) if len(word) >= min_length])
                    break
                except ValueError:
                    continue
    else:
        for article in articles:
            words.update([word for word in re.findall(r'\b\w+\b', article['content'].lower()) if len(word) >= min_length])

    return words


# Funkce pro získání 8 nejčastějších slov v článcích
def most_common_words(articles):
    words = process_words(articles)
    common_words = words.most_common(8)
    return '8 nejčastějších slov v článcích:\n' + '\n'.join([f'{word}: {count}x' for word, count in common_words])

# Funkce pro získání 5 nejčastějších slov z roku 2022
def most_common_words_2022(articles):
    words = process_words(articles, min_length=1, year=2022)
    common_words = words.most_common(5)
    return '5 nejčastějších slov v článcích z roku 2022:\n' + '\n'.join([f'{word}: {count}x' for word, count in common_words])

# vypiste 3 clanky s nejvetsim poctem vyskytu slova "covid-19"
def most_covid_articles(articles):
    words = process_words(articles, min_length=1)
    covid_articles = []
    for article in articles:
        covid_count = len(re.findall(r'\bcovid-19\b', article['content'].lower()))
        if covid_count > 0:
            covid_articles.append((article['title'], covid_count))
    covid_articles.sort(key=lambda x: x[1], reverse=True)
    return '\n'.join([f'{title}: {count}x' for title, count in covid_articles[:3]])

# vypiste clanky s nejvetsim a nejmensim poctem slov
def most_words(articles):
    word_counts = {article['title']: len(re.findall(r'\b\w+\b', article['content'])) for article in articles}
    most_words_article = max(word_counts, key=word_counts.get)
    least_words_article = min(word_counts, key=word_counts.get)
    return f'Článek s nejvíce slovy: {most_words_article}\nČlánek s nejméně slovy: {least_words_article}'

#vyste prumernou delku slov pres vsechny clanky
def average_word_length(articles):
    words = process_words(articles, min_length=1)
    total_length = sum([len(word) for word in words])
    return f'Průměrná délka slova: {total_length / len(words)}'

#vypiste mesice s nejvice a nejmene pulikovanych clanku
def most_published_months(articles):
    date_formats = ['%Y-%m-%d', '%d. %m. %Y %H:%M']
    months = []
    for article in articles:
        for date_format in date_formats:
            try:
                months.append(datetime.strptime(article['publication_date'], date_format).month)
                break
            except ValueError:
                continue
    month_counts = Counter(months)
    most_published_month = max(month_counts, key=month_counts.get)
    least_published_month = min(month_counts, key=month_counts.get)
    return f'Měsíc s nejvíce publikovanými články: {most_published_month}\nMěsíc s nejméně publikovanými články: {least_published_month}'

if __name__ == '__main__':
    with open('scraped_articles.jsonl', 'r', encoding='utf-8') as file:
        articles = [json.loads(line) for line in file]
        
    # print(count_articles(articles))
    # print(count_duplicate_articles(articles))
    # print(oldest_article_date(articles))
    # print(most_commented_article(articles))
    # print(most_images(articles))
    # print(articles_by_year(articles))
    # print(unique_categories(articles))
    # print(most_common_words_2022(articles))
    # print(total_comments(articles))
    # print(total_words(articles))
    
    # # bonus
    # print(most_common_words(articles))
    # print(most_covid_articles(articles))
    # print(most_words(articles))
    # print(average_word_length(articles))
    # print(most_published_months(articles))
    
    with open('output.txt', 'w', encoding='utf-8') as output_file:
        output_file.write(count_articles(articles) + '\n')
        output_file.write(count_duplicate_articles(articles) + '\n')
        output_file.write(oldest_article_date(articles) + '\n')
        output_file.write(most_commented_article(articles) + '\n')
        output_file.write(most_images(articles) + '\n')
        output_file.write(articles_by_year(articles) + '\n')
        output_file.write(unique_categories(articles) + '\n')
        output_file.write(most_common_words_2022(articles) + '\n')
        output_file.write(total_comments(articles) + '\n')
        output_file.write(total_words(articles) + '\n')
        
        # bonus
        output_file.write(most_common_words(articles) + '\n')
        output_file.write(most_covid_articles(articles) + '\n')
        output_file.write(most_words(articles) + '\n')
        output_file.write(average_word_length(articles) + '\n')
        output_file.write(most_published_months(articles) + '\n')
    
    
    
    