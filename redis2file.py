import redis
from itertools import zip_longest
import re
import pandas as pd
import bs4
r = redis.StrictRedis(host='localhost', port=26379, db=0)

# iterate a list in batches of size n


def batcher(iterable, n):
    args = [iter(iterable)] * n
    return zip_longest(*args)


# in batches of 500 delete keys matching user:*
for keybatch in batcher(r.scan_iter('https://www.ted.com/speakers/*'), 500):
    # r.delete(*keybatch)
    print(keybatch)

data = []
first = None
for key in r.scan_iter('https://www.ted.com/speakers/*'):
    name = key.decode('utf-8').split('/')[-1]
    value = r.get(key).decode('utf-8')
    he_count = len(re.findall(r'\b[hH][Ee]\b', value))
    she_count = len(re.findall(r'\b[sS][hH][eE]\b', value))

    soup = bs4.BeautifulSoup(value, "lxml")
    h1 = soup.find('h1', class_='h2 profile-header__name')
    print(name, he_count, she_count, h1.text)
    data.append((name, he_count, she_count, h1.text))
    if first is None:
        first = value

# print(first)
df = pd.DataFrame(data, columns=['name_in_url',
                                 'he_count', 'she_count', 'name_in_profile'])
df.to_csv('ted_speaker_gender.csv')
print(df.count())

# cache_file = 'cache/' + get_filename(url)
# # if os.path.exists(cache_file):
#                    with open(cache_file, 'r') as f:
#                         html = f.read()
#                        os.remove(cache_file)
#                     # async with aiofiles.open(cache_file, 'w') as f:
#                     #     await f.wirte(html)
