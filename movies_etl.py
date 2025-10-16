#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL de películas: une metadatos (id/género/director) con top_rated y genera tablas limpias.
Entradas esperadas:
  - movies_id_genre_director_.csv
  - top_rated_movies.csv
Salidas:
  - analytics_movies/movies_clean.csv
  - analytics_movies/movies_genres_exploded.csv
  - analytics_movies/movies_directors_exploded.csv
  - analytics_movies/yearly_counts.csv
  - analytics_movies/genre_year_counts.csv
  - analytics_movies/decade_top_voted.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

def parse_year(s):
    if pd.isna(s):
        return np.nan
    s = str(s)
    if s.isdigit() and len(s) == 4:
        return int(s)
    return pd.to_datetime(s, errors="coerce").year

def to_list(x):
    if pd.isna(x):
        return []
    s = str(x).strip()
    if s == "" or s.lower() == "none":
        return []
    if s.startswith("[") and s.endswith("]"):
        s2 = s.strip("[]")
        parts = [p.strip().strip("'").strip('"') for p in s2.split(",") if p.strip()]
        return [p for p in parts if p]
    if "|" in s:
        return [p.strip() for p in s.split("|") if p.strip()]
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s]

def std_cols(df):
    return {c.lower().strip(): c for c in df.columns}

def pick(colmap, candidates):
    for cand in candidates:
        for c in colmap:
            if c == cand or cand in c:
                return colmap[c]
    return None

def run_etl(movies_meta_path, top_rated_path, out_dir="analytics_movies"):
    movies_meta = pd.read_csv(movies_meta_path)
    top_rated = pd.read_csv(top_rated_path)

    cols_meta = std_cols(movies_meta)
    cols_top = std_cols(top_rated)

    id_col_meta = pick(cols_meta, ["id","movie_id","imdb_id","tmdb_id"])
    id_col_top  = pick(cols_top, ["id","movie_id","imdb_id","tmdb_id"])

    title_meta  = pick(cols_meta, ["title","original_title","name"])
    title_top   = pick(cols_top, ["title","original_title","name"])

    date_meta   = pick(cols_meta, ["release_date","date","year"])
    date_top    = pick(cols_top, ["release_date","date","year"])

    pop_top     = pick(cols_top, ["popularity","score_popularity","pop"])
    votes_top   = pick(cols_top, ["vote_count","votes","num_votes"])
    voteavg_top = pick(cols_top, ["vote_average","rating","vote_avg","avg_vote"])

    genres_col = pick(cols_meta, ["genres","genre","generos","topics"])
    directors_col = pick(cols_meta, ["director","directors"])

    m = movies_meta.copy()
    if id_col_meta and id_col_meta in m.columns:
        m = m.rename(columns={id_col_meta:"movie_id"})
    else:
        m["movie_id"] = range(1, len(m)+1)

    if title_meta and title_meta in m.columns:
        m = m.rename(columns={title_meta:"title"})
    else:
        m["title"] = m["movie_id"].astype(str)

    if date_meta and date_meta in m.columns:
        m["release_year"] = m[date_meta].apply(parse_year)
    else:
        m["release_year"] = np.nan

    if genres_col and genres_col in m.columns:
        m = m.rename(columns={genres_col:"genres"})
    else:
        m["genres"] = None

    if directors_col and directors_col in m.columns:
        m = m.rename(columns={directors_col:"directors"})
    else:
        m["directors"] = None

    m_base = m[["movie_id","title","release_year","genres","directors"]].copy()

    t = top_rated.copy()
    if id_col_top and id_col_top in t.columns:
        t = t.rename(columns={id_col_top:"movie_id"})
    else:
        if title_top and title_top in t.columns:
            t = t.rename(columns={title_top:"title"})
        else:
            t["title"] = t.index.astype(str)

    if title_top and "title" not in t.columns:
        t = t.rename(columns={title_top:"title"})

    if date_top and date_top in t.columns and "release_year" not in t.columns:
        t["release_year"] = t[date_top].apply(parse_year)

    if pop_top and pop_top in t.columns:
        t = t.rename(columns={pop_top:"popularity"})
    if votes_top and votes_top in t.columns:
        t = t.rename(columns={votes_top:"vote_count"})
    if voteavg_top and voteavg_top in t.columns:
        t = t.rename(columns={voteavg_top:"vote_average"})

    keep_cols = [c for c in ["movie_id","title","release_year","popularity","vote_count","vote_average"] if c in t.columns]
    t = t[keep_cols].copy()

    if "movie_id" in t.columns:
        movies = m_base.merge(t, on=["movie_id"], how="left", suffixes=("","_t"))
        if "title_t" in movies.columns:
            movies["title"] = movies["title"].fillna(movies["title_t"])
        if "release_year_t" in movies.columns:
            movies["release_year"] = movies["release_year"].fillna(movies["release_year_t"])
    else:
        movies = m_base.merge(t, on=["title"], how="left")

    movies["genres_list"] = movies["genres"].apply(to_list)
    movies["directors_list"] = movies["directors"].apply(to_list)

    for c in ["popularity","vote_count","vote_average","release_year"]:
        if c in movies.columns:
            movies[c] = pd.to_numeric(movies[c], errors="coerce")

    movies["decade"] = (movies["release_year"]//10*10).astype("Int64")

    genres_expl = movies[["movie_id","title","release_year","popularity","vote_count","vote_average","genres_list"]].explode("genres_list")
    genres_expl = genres_expl.rename(columns={"genres_list":"genre"})

    directors_expl = movies[["movie_id","title","release_year","popularity","vote_count","vote_average","directors_list"]].explode("directors_list")
    directors_expl = directors_expl.rename(columns={"directors_list":"director"})

    count_by_year = movies.groupby("release_year", dropna=True, as_index=False).size().rename(columns={"size":"count"})
    genre_year_counts = genres_expl.dropna(subset=["genre","release_year"]).groupby(["release_year","genre"]).size().reset_index(name="count")
    decade_top_voted = movies.dropna(subset=["decade"]).sort_values(["decade","vote_count"], ascending=[True, False])         .groupby("decade", as_index=False).first()[["decade","movie_id","title","vote_count","vote_average","popularity","release_year"]]

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    movies.to_csv(out_dir / "movies_clean.csv", index=False)
    genres_expl.to_csv(out_dir / "movies_genres_exploded.csv", index=False)
    directors_expl.to_csv(out_dir / "movies_directors_exploded.csv", index=False)
    count_by_year.to_csv(out_dir / "yearly_counts.csv", index=False)
    genre_year_counts.to_csv(out_dir / "genre_year_counts.csv", index=False)
    decade_top_voted.to_csv(out_dir / "decade_top_voted.csv", index=False)

if __name__ == "__main__":
    run_etl("movies_id_genre_director_.csv", "top_rated_movies.csv")
