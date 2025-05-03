import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os

def save_nba_news():
    url = 'https://www.sportingnews.com/mx/nba/los-angeles-lakers/news/lakers-eliminacion-motivos-playoffs/2feaed55791e8b9dad749a04'
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    article = soup.find('div', {'data-testid': 'article-content-body'})

    if article:
        article_text = article.get_text(separator=' ', strip=True)
    else:
        article_text = "No article found."

    output_dir = '/opt/airflow/nba_data/news-logs'
    file_name = f"nba_news_{datetime.now().strftime('%Y%m%d')}.txt"
    file_path = os.path.join(output_dir, file_name)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(article_text)
    
    print(f"Article saved to {file_path}")


