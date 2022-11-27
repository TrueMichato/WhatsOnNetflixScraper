import bs4
import pandas as pd
import numpy as np
import imdb
import sys
import logging
from pandarallel import pandarallel
from tqdm import tqdm

from uri_extractor import UriExtractor
from datetime import date, datetime, timedelta

BASE_URL = "https://www.whats-on-netflix.com/most-popular/?dateselect="
NAME_REGEX = r'(?P<name>[\w\ \-\:\,\'\?\!\&\#\$\.]+)'
POINTS_REGEX = r'(?P<points>\(\d{1,10} \w+ points\))'

logging.basicConfig(filename="log2.txt", level=logging.WARNING)

urls = []
begin_date = date(2020, 11, 4)
end_date = date.today()


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


for single_date in daterange(begin_date, end_date):
    urls.append(f"{BASE_URL}{single_date.month}%2F{single_date.day}%2F{single_date.year}")


def extract_tables(url_dict):
    cols = ['name', 'country', 'rating', 'date', 'points', 'url', 'genres']
    genres = [f'genre {i}' for i in range(1, 11)]
    cols = cols + genres
    df_dict = {'Movies': pd.DataFrame(columns=cols), 'TV': pd.DataFrame(columns=cols), 'Kids': pd.DataFrame(columns=cols)}
    pandarallel.initialize(progress_bar=False)
    ia = imdb.Cinemagoer()
    for url, html in tqdm(url_dict.items()):
        soup = bs4.BeautifulSoup(html, features="lxml")
        h4_titles = soup.find_all("h4")
        tables = soup.findAll("table")
        for title, table in zip(h4_titles, tables):
            country = title.string.split(" Top ")[0]
            tb = pd.read_html(str(table).replace("TV Series", "TV").replace("TV Shows", "TV"))[0]
            points = False
            for dftype in df_dict.keys():
                if dftype in tb.columns and not tb.empty:
                    if not tb[dftype].isnull().values.any():
                        if tb[dftype].str.contains('points').any():
                            temp = tb[dftype].str.extract(f'{NAME_REGEX}{POINTS_REGEX}')
                            points = True
                        else:
                            temp = tb[dftype].str.extract(f'{NAME_REGEX}')
                        temp['rating'] = temp.index + 1
                        temp['country'] = country.strip()
                        temp['date'] = url[1]
                        temp['url'] = url[0]
                        if points:
                            temp['points'] = temp.points.str.extract(r'(?P<points>\d+)')
                        else:
                            temp['points'] = np.nan
                        temp['name'] = temp.name.str.strip()
                        temp['genres'] = temp['name'].apply(find_movie_genre, args=(df_dict, ia,))
                        temp_genre = temp.genres.str.split(',', expand=True)
                        for i in range(len(temp_genre.columns), len(genres)):
                            temp_genre[i] = None
                        temp[genres] = temp_genre
                        temp = temp[cols]
                        df_dict[dftype] = pd.concat([df_dict.get(dftype), temp], ignore_index=True)
    df_dict['Movies'].to_csv(f'dataset_top_netflix_movies_{begin_date}_{end_date}.csv', sep=',', header=True, index=False, columns=cols)
    df_dict['TV'].to_csv(f'dataset_top_netflix_shows_{begin_date}_{end_date}.csv', sep=',', header=True, index=False,
                     columns=cols)
    df_dict['Kids'].to_csv(f'dataset_top_netflix_kids_{begin_date}_{end_date}.csv', sep=',', header=True, index=False,
                    columns=cols)
    return df_dict


def find_movie_genre(movie_name, df_dict, ia):
    existing = check_existing(movie_name, df_dict)
    if existing:
        return ','.join(existing)
    movies = ia.search_movie(movie_name)
    retry = 0
    while not movies and retry < 10:
        movies = ia.search_movie(movie_name)
        retry += 1
    if not movies:
        return ''
    try:
        movie = [mov for mov in movies if movie_name.lower() == mov['title'].lower()]
        if not movie:
            try:
                movie = [mov for mov in movies if movie_name.lower() in list(map(str.lower, mov.get('akas', "")))][0]
            except Exception as ke:
                # logging.warning(f"Genre matching got following exception but continued:\n {ke}")
                try:
                    movie = [mov for mov in movies if movie_name.lower() in mov['title'].lower()][0]
                except Exception as e:
                    # logging.warning(f"Genre matching got following exception but continued:\n {e}")
                    movie = choose_by_words(movie_name, movies)
        else:
            movie = movie[0]
        movie = ia.get_movie(movie.movieID)
        return ','.join(movie['genres'])
    except Exception as e:
        # t, v, tb = sys.exc_info()
        # logging.debug(f"movie name from table: {movie_name.lower()}\n movies names from title: {[mov['title'].lower() for mov in movies]}")
        # logging.warning(f"Genre matching got following exception and failed:\n {e}\n traceback: \n {t(v).with_traceback(tb)}")
        return ''


def choose_by_words(movie_name, movies_list):
    movie_title_words = movie_name.count(" ")
    movies_title_words = [mov['title'].count(" ") for mov in movies_list]
    mini = 99999
    choose = -1
    for i in range(len(movies_title_words)):
        if abs(movie_title_words - movies_title_words[i]) < mini:
            mini = abs(movie_title_words - movies_title_words[i])
            choose = i
    logging.debug(
        f"movie name from table: {movie_name.lower()}\n movies names from title: {[mov['title'].lower() for mov in movies_list]}")
    return movies_list[choose]


def check_existing(movie_name, df_dict):
    for key, df in df_dict.items():
        temp = df[df['name'] == movie_name]
        if not temp.empty:
            return temp['genres'][temp.first_valid_index()].split(',')
        return []


def attach_genre(dfs):
    ia = imdb.Cinemagoer()
    for key, df in dfs.items():
        df['imdb'] = df['name'].apply(ia.search_movie)[0]
        df['imdb'] = df['imdb'].apply(ia.get_movie)
        df['genres'] = df['imdb'].apply(lambda movie: movie.get('genres'))
        df.drop('imdb', inplace=True)


if __name__ == '__main__':
    extractor = UriExtractor(urls=urls)
    res = extractor.run()
    tables = extract_tables(res)
    # attach_genre(tables)


