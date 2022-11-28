from datetime import timedelta
import sys
import logging
import imdb


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def check_existing(movie_name, df_dict):
    for key, df in df_dict.items():
        temp = df[df['name'] == movie_name]
        if not temp.empty:
            return temp['genres'][temp.first_valid_index()].split(',')
        return []


def find_movie_genre(movie_name):
    ia = imdb.Cinemagoer()
    try:
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
            logging.debug(
                f"movie name from table: {movie_name.lower()}\n movies names from title: {[mov['title'].lower() for mov in movies]}")
            logging.warning(
                f"Genre matching got following exception and failed:\n {e}\n traceback: \n {t(v).with_traceback(tb)}")
            return {movie_name: ''}
    except Exception as e:
        t, v, tb = sys.exc_info()
        logging.debug(
            f"movie name from table: {movie_name.lower()}\n")
        logging.warning(
            f"Genre matching got following exception and failed:\n {e}\n traceback: \n {t(v).with_traceback(tb)}")
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

