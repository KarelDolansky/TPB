import json
import matplotlib.pyplot as plt
from datetime import datetime
from collections import Counter
from pymongo import MongoClient

# Připojení k MongoDB
client = MongoClient('mongodb://localhost:27018/')
db = client['articles']
collection = db['data']

def save_plot(fig, filename, title):
    """Uloží a uzavře graf s daným názvem a titulem."""
    fig.suptitle(title)
    fig.savefig(filename)
    plt.close(fig)
    print(f"Graf uložen jako '{filename}'")

def parse_date(date_str):
    """Parsování data ze zadaného formátu."""
    for fmt in ('%Y-%m-%d', '%d. %m. %Y %H:%M', '%b %d, %Y v %H:%M %Z'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def gather_article_data():
    """Shromáždí potřebná data přímo z MongoDB po částech."""
    dates, lengths, categories, comments = [], [], [], []

    # Omezit počet načítaných polí (projection)
    projection = {
        'publication_date': 1,
        'content': 1,
        'category': 1,
        'comment_count': 1
    }

    # Zpracovávat články po částech
    cursor = collection.find({}, projection).batch_size(1000)  # Zpracování po 1000 dokumentů
    for article in cursor:
        publication_date = parse_date(article.get('publication_date'))
        if publication_date:
            dates.append(publication_date)
            lengths.append(len(article['content'].split()))
            categories.append(article['category'])
            comments.append(article['comment_count'])

    return sorted(dates), lengths, categories, comments

def plot_articles_over_time(dates):
    """Vykreslí časovou osu počtu článků v čase."""
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, range(1, len(dates) + 1), marker='o')
    ax.set(xlabel='Datum', ylabel='Počet článků')
    save_plot(fig, 'krivka_pridani_clanku_v_case.png', 'Křivka přidání článků v čase')

def plot_articles_per_year(dates):
    """Vykreslí sloupcový graf počtu článků v jednotlivých rocích."""
    years = Counter(date.year for date in dates)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(years.keys(), years.values())
    ax.set(xlabel='Rok', ylabel='Počet článků')
    save_plot(fig, 'pocet_clanku_v_jednotlivych_rocich.png', 'Počet článků v jednotlivých letech')

def plot_length_vs_comments(lengths, comments):
    """Scatter graf vztahu mezi délkou článku a počtem komentářů."""
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(lengths, comments)
    ax.set(xlabel='Počet slov v článku', ylabel='Počet komentářů')
    save_plot(fig, 'vztah_delky_clanku_a_poctu_komentaru.png', 'Vztah mezi délkou článku a počtem komentářů')

def plot_category_distribution(categories):
    """Koláčový graf podílu článků podle kategorií."""
    category_counts = Counter(categories)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.pie(category_counts.values(), labels=category_counts.keys(), autopct='%1.1f%%')
    save_plot(fig, 'podil_clanku_v_jednotlivych_kategoriich.png', 'Podíl článků v jednotlivých kategoriích')

def plot_word_histogram(lengths):
    """Histogram počtu slov v článcích."""
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(lengths, bins=20)
    ax.set(xlabel='Počet slov v článku', ylabel='Počet článků')
    save_plot(fig, 'histogram_pro_pocet_slov_v_clancich.png', 'Histogram pro počet slov v článcích')

def plot_weekday_histogram(dates):
    """Histogram pro počet článků podle dne v týdnu."""
    weekdays = [date.weekday() for date in dates]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(weekdays, bins=7, rwidth=0.8, align='left')
    ax.set_xticks(range(7))
    ax.set_xticklabels(['Po', 'Út', 'St', 'Čt', 'Pá', 'So', 'Ne'])
    ax.set(xlabel='Den v týdnu', ylabel='Počet článků')
    save_plot(fig, 'histogram_pro_pocet_clanku_v_jednotlivych_dnech_tyden.png', 'Histogram pro počet článků v jednotlivých dnech týdne')

def get_random_article():
    """Vyhledá a vypíše náhodný článek."""
    try:
        article = collection.aggregate([{"$sample": {"size": 1}}]).next()
        print("Náhodný článek:", article)
    except StopIteration:
        print("Nebyly nalezeny žádné články.")

def get_total_articles():
    """Vrátí celkový počet článků."""
    count = collection.count_documents({})
    print("Celkový počet článků:", count)

def get_average_image_count():
    """Vypočítá průměrný počet fotografií na článek."""
    pipeline = [
        {
            "$group": {
                "_id": None,
                "avgImageCount": {
                    "$avg": {
                        "$cond": [
                            {"$ifNull": ["$image_count", 0]},
                            "$image_count",
                            0
                        ]
                    }
                }
            }
        }
    ]
    result = next(collection.aggregate(pipeline), {'avgImageCount': 0})
    print("Průměrný počet fotek na článek:", result['avgImageCount'])

def get_articles_with_comments_over_100():
    """Vrátí počet článků s více než 100 komentáři."""
    count = collection.count_documents({"comment_count": {"$gt": 100}})
    print("Počet článků s více než 100 komentáři:", count)

def get_articles_count_by_category_2022():
    """Vrátí počet článků pro každou kategorii v roce 2022."""
    pipeline = [
        {
            "$match": {
                "publication_date": {
                    "$gte": "2022-01-01",
                    "$lt": "2023-01-01"
                }
            }
        },
        {
            "$group": {
                "_id": "$category",
                "count": {"$sum": 1}
            }
        }
    ]
    results = collection.aggregate(pipeline)
    for result in results:
        print(f"Kategorie: {result['_id']}, Počet článků: {result['count']}")

if __name__ == '__main__':
    # Zkontrolujte, zda kolekce není prázdná
    if collection.count_documents({}) == 0:
        print("Kolekce je prázdná.")
        exit()

    # Shromáždění dat článků
    dates, lengths, categories, comments = gather_article_data()

    # Vykreslení grafů
    plot_articles_over_time(dates)
    plot_articles_per_year(dates)
    plot_length_vs_comments(lengths, comments)
    plot_category_distribution(categories)
    plot_word_histogram(lengths)
    plot_weekday_histogram(dates)

    # bonus
    get_random_article()
    get_total_articles()
    get_average_image_count()
    get_articles_with_comments_over_100()
    get_articles_count_by_category_2022()
