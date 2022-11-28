import bs4
import pandas as pd
import numpy as np
from tqdm import tqdm
import multiprocessing as mp
from functools import reduce
from defaults import *
from helper_functions import *

from uri_extractor import UriExtractor
from datetime import date


class RunManager:

    def __init__(self, begin, end, logfile_name="log2"):
        self.start_date = begin
        self.end_date = end
        self.urls = [f"{BASE_URL}{sg.month}%2F{sg.day}%2F{sg.year}" for sg in daterange(self.start_date, self.end_date)]
        self.url_dict = {}
        self.df_dict = {}
        logging.basicConfig(filename=f"{logfile_name}.txt", level=logging.WARNING)

    def run_crawler(self):
        extractor = UriExtractor(urls=self.urls)
        self.url_dict = extractor.run()

    def run_extraction(self):
        if not self.url_dict:
            self.run_crawler()
        self.extract_tables()

    def run_genre_matching(self):
        if self.df_dict:
            self.attach_genre()
        elif not self.url_dict:
            try:
                self.df_dict['Movies'] = pd.read_csv(
                    f'dataset_top_netflix_movies_{self.start_date}_{self.end_date}.csv')
                self.df_dict['TV'] = pd.read_csv(
                    f'dataset_top_netflix_shows_{self.start_date}_{self.end_date}.csv')
                self.df_dict['Kids'] = pd.read_csv(
                    f'dataset_top_netflix_kids_{self.start_date}_{self.end_date}.csv')
            except Exception as e:
                self.run_crawler()
                self.extract_tables()
            finally:
                self.attach_genre()

    def extract_tables(self):
        cols = ['name', 'country', 'rating', 'date', 'points', 'url']
        self.df_dict = {'Movies': pd.DataFrame(columns=cols), 'TV': pd.DataFrame(columns=cols),
                        'Kids': pd.DataFrame(columns=cols)}
        for url, html in tqdm(self.url_dict.items()):
            soup = bs4.BeautifulSoup(html, features="lxml")
            h4_titles = soup.find_all("h4")
            tables = soup.findAll("table")
            for title, table in zip(h4_titles, tables):
                country = title.string.split(" Top ")[0]
                tb = pd.read_html(str(table).replace("TV Series", "TV").replace("TV Shows", "TV"))[0]
                points = False
                for dftype in self.df_dict.keys():
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
                            self.df_dict[dftype] = pd.concat([self.df_dict.get(dftype), temp], ignore_index=True)
        self.df_dict['Movies'].to_csv(f'dataset_top_netflix_movies_{self.start_date}_{self.end_date}.csv', sep=',',
                                      header=True, index=False, columns=cols)
        self.df_dict['TV'].to_csv(f'dataset_top_netflix_shows_{self.start_date}_{self.end_date}.csv', sep=',',
                                  header=True, index=False, columns=cols)
        self.df_dict['Kids'].to_csv(f'dataset_top_netflix_kids_{self.start_date}_{self.end_date}.csv', sep=',',
                                    header=True, index=False,  columns=cols)

    def attach_genre(self):
        cols = ['name', 'country', 'rating', 'date', 'points', 'url', 'genres']
        genres_list = [f'genre {i}' for i in range(1, 26)]
        cols = cols + genres_list
        for key, df in self.df_dict.items():
            unique_names = df['name'].unique()
            n_processors = mp.cpu_count()
            pool = mp.Pool(n_processors)
            genres = pool.map(find_movie_genre, unique_names)
            pool.close()
            genres_dict = reduce(lambda a, b: a | b, genres)
            df['genres'] = df['name'].apply(lambda a: genres_dict.get(a, ""))
            temp_genre = df.genres.str.split(',', expand=True)
            for i in range(len(temp_genre.columns), len(genres_list)):
                temp_genre[i] = None
            df[genres_list] = temp_genre
            df.to_csv(f'dataset_top_netflix_{key}_{self.start_date}_{self.end_date}_genres.csv', sep=',', header=True,
                      index=False, columns=cols)


if __name__ == '__main__':
    begin_date = date(2020, 11, 4)
    end_date = date.today()
    manager = RunManager(begin_date, end_date)
    manager.run_crawler()
    manager.run_extraction()
    manager.run_genre_matching()
