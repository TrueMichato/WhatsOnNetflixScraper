import bs4
import pandas as pd
import numpy as np
import imdb
import sys
import logging
from tqdm import tqdm
import multiprocessing as mp
from functools import reduce


from uri_extractor import UriExtractor
from datetime import date, datetime, timedelta

BASE_URL = "https://www.whats-on-netflix.com/most-popular/?dateselect="
NAME_REGEX = r'(?P<name>[\w\ \-\:\,\'\?\!\&\#\$\.]+)'
POINTS_REGEX = r'(?P<points>\(\d{1,10} \w+ points\))'

logging.basicConfig(filename="log2.txt", level=logging.WARNING)

urls = []
begin_date = date(2022, 11, 4)
end_date = date.today()


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


for single_date in daterange(begin_date, end_date):
    urls.append(f"{BASE_URL}{single_date.month}%2F{single_date.day}%2F{single_date.year}")


def extract_tables(url_dict):
    cols = ['name', 'country', 'rating', 'date', 'points', 'url']
    df_dict = {'Movies': pd.DataFrame(columns=cols), 'TV': pd.DataFrame(columns=cols), 'Kids': pd.DataFrame(columns=cols)}
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
                        temp = temp[cols]
                        df_dict[dftype] = pd.concat([df_dict.get(dftype), temp], ignore_index=True)
    df_dict['Movies'].to_csv(f'dataset_top_netflix_movies_{begin_date}_{end_date}.csv', sep=',', header=True, index=False, columns=cols)
    df_dict['TV'].to_csv(f'dataset_top_netflix_shows_{begin_date}_{end_date}.csv', sep=',', header=True, index=False,
                     columns=cols)
    df_dict['Kids'].to_csv(f'dataset_top_netflix_kids_{begin_date}_{end_date}.csv', sep=',', header=True, index=False,
                    columns=cols)
    return df_dict


def find_movie_genre(movie_name):
    ia = imdb.Cinemagoer()
    movies = ia.search_movie(movie_name)
    retry = 0
    while not movies and retry < 10:
        movies = ia.search_movie(movie_name)
        retry += 1
    if not movies:
        return {movie_name: ''}
    try:
        movie = [mov for mov in movies if movie_name.lower() == mov['title'].lower()]
        if not movie:
            try:
                movie = [mov for mov in movies if movie_name.lower() in list(map(str.lower, mov.get('akas', "")))][0]
            except Exception as ke:
                logging.warning(f"Genre matching got following exception but continued:\n {ke}")
                try:
                    movie = [mov for mov in movies if movie_name.lower() in mov['title'].lower()][0]
                except Exception as e:
                    logging.warning(f"Genre matching got following exception but continued:\n {e}")
                    movie = choose_by_words(movie_name, movies)
        else:
            movie = movie[0]
        movie = ia.get_movie(movie.movieID)
        return {movie_name: ','.join(movie['genres'])}
    except Exception as e:
        t, v, tb = sys.exc_info()
        logging.debug(f"movie name from table: {movie_name.lower()}\n movies names from title: {[mov['title'].lower() for mov in movies]}")
        logging.warning(f"Genre matching got following exception and failed:\n {e}\n traceback: \n {t(v).with_traceback(tb)}")
        return {movie_name: ''}


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
    cols = ['name', 'country', 'rating', 'date', 'points', 'url', 'genres']
    genres_list = [f'genre {i}' for i in range(1, 11)]
    cols = cols + genres_list
    for key, df in dfs.items():
        unique_names = df['name'].unique()
        n_processors = mp.cpu_count()
        pool = mp.Pool(n_processors)
        genres = pool.map(find_movie_genre, unique_names)
        genres_dict = reduce(lambda a, b: a | b, genres)
        df['genres'] = df['name'].apply(lambda a: genres_dict[a])
        temp_genre = df.genres.str.split(',', expand=True)
        for i in range(len(temp_genre.columns), len(genres_list)):
            temp_genre[i] = None
        df[genres_list] = temp_genre
        df.to_csv(f'dataset_top_netflix_{key}_{begin_date}_{end_date}_genres.csv', sep=',', header=True, index=False,
                  columns=cols)


if __name__ == '__main__':
    extractor = UriExtractor(urls=urls)
    res = extractor.run()
    tables = extract_tables(res)
    attach_genre(tables)


