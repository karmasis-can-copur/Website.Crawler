import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import os
import datetime
from colorama import Fore, Style, init

# Colorama başlatma
init(autoreset=True)

# Dosyadan URL'ler ve keyword'ler alınıyor
with open('urls.txt', 'r') as f:
    url_list = [line.strip() for line in f.readlines()]

with open('keywords.txt', 'r') as f:
    lookup_keywords = [line.strip() for line in f.readlines()]

# Sonuçları depolamak için set kullanıyoruz (unique olması için)
results = set()

# Tarih formatı ve dosya isimlendirme
current_date = datetime.date.today()
date_format = current_date.strftime('%Y_%m_%d')
result_file = f"{date_format}.csv"

# Geçmiş dosyaları yükleyip, önceki sonuçları okuma
previous_results = set()
checked_files = set()

for i in range(7):  # Son 7 gün kontrol edilecek
    date_to_check = (current_date - datetime.timedelta(days=i)).strftime('%Y_%m_%d')
    file_to_check = f"{date_to_check}.csv"
    if os.path.exists(file_to_check):
        checked_files.add(file_to_check)
        with open(file_to_check, 'r') as f:
            for line in f:
                previous_results.add(tuple(line.strip().split(',')))

# Mevcut çalıştırmada eklenen sonuçları tutmak için set
current_results = set()

# Her bir dosyadaki sonuçları tekrar kontrol etme
file_summary = {}
for file in checked_files:
    with open(file, 'r') as f:
        records = [line.strip().split(',') for line in f.readlines()]
        file_summary[file] = len(records)
        for record in records:
            current_results.add(tuple(record))


def process_url(url):
    try:
        # Sayfa kaynağını getiriyoruz
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Sayfa kaynak kodunu alıyoruz
        source_code = response.text.lower()

        # Linkleri çekmek için BeautifulSoup kullanıyoruz
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].startswith("http")]

        # Her bir keyword ve link'i kontrol ediyoruz
        for keyword in lookup_keywords:
            for link in links:
                if keyword.lower() in link.lower():
                    result = (url, keyword, link, date_format)
                    if result not in previous_results and result not in current_results:  # Yeni sonuç kontrolü
                        results.add(result)
                        current_results.add(result)  # Şu anki çalıştırma içinde ekleneni takip et
                        print(Fore.YELLOW + f"Found new link: {result}")

    except Exception as e:
        print(f"Error processing {url}: {e}")

# Çoklu iş parçacığı kullanarak hızlandırma yapıyoruz
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(process_url, url_list)

# Sonuçları CSV formatında dosyaya kaydediyoruz
with open(result_file, 'a') as f:
    for result in results:
        f.write(','.join(result) + '\n')

# Özet rapor
print(Fore.GREEN + "Summary of existing records:")
for file, count in file_summary.items():
    print(Fore.GREEN + f"{file}: {count} records")

if not results:
    print(Fore.RED + "No new records found!!")
else:
    print(Fore.YELLOW + f"{len(results)} new records added.")
