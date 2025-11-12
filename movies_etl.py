#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Intentar importar mysql.connector, si no está instalado dejamos un marcador
try:
    import mysql.connector
    from mysql.connector import Error
except Exception:
    mysql = None
    Error = Exception

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MoviesETL:
    """
    ETL Process para unificar datos de películas y cargarlos a MySQL Data Warehouse
    Incluye estadísticas por género y por director.
    """
    
    def __init__(self, top_rated_path, id_genre_director_path, db_config):
        """
        Inicializa el ETL con las rutas de los archivos CSV y la configuración de la BD
        
        Args:
            top_rated_path: Ruta al archivo top_rated_movies.csv
            id_genre_director_path: Ruta al archivo movies_id_genre_director_.csv
            db_config: Diccionario con la configuración de MySQL
        """
        self.top_rated_path = top_rated_path
        self.id_genre_director_path = id_genre_director_path
        self.db_config = db_config
        
        # Inicializar conexión
        self.connection = None
        
        # Inicializar DataFrames
        self.df_top_rated = None
        self.df_genre_director = None
        self.df_unified = None

        # DataFrames de estadísticas
        self.genre_stats = None
        self.director_stats = None
    
    def connect_to_db(self):
        """
        Establecer conexión con MySQL.
        En CADA ejecución:
        - Elimina la base de datos indicada en db_config['database'] si existe.
        - La vuelve a crear vacía.
        - Se conecta a esa base recién creada.
        """
        if mysql is None:
            logger.error("❌ mysql.connector no está disponible; instala el paquete con: pip install mysql-connector-python")
            return False

        host = self.db_config['host']
        user = self.db_config['user']
        password = self.db_config['password']
        port = self.db_config.get('port', 3306)
        db_name = self.db_config['database']

        try:
            # 1) Conexión sin base para poder dropear y crear
            logger.info(f"💣 Dropeando y recreando base de datos '{db_name}'...")
            base_conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                port=port
            )
            base_cursor = base_conn.cursor()
            # Drop + Create
            base_cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`;")
            base_cursor.execute(
                f"CREATE DATABASE `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
            base_conn.commit()
            base_cursor.close()
            base_conn.close()
            logger.info(f"  ✓ Base de datos '{db_name}' creada desde cero")

            # 2) Ahora sí, conectar usando esa base limpia
            self.connection = mysql.connector.connect(
                host=host,
                database=db_name,
                user=user,
                password=password,
                port=port
            )
            
            if self.connection.is_connected():
                logger.info("✓ Conexión exitosa a MySQL (con base de datos recién creada)")
                return True

        except Error as e:
            logger.error(f"❌ Error conectando/creando base de datos MySQL: {e}")
            return False
    
    def close_connection(self):
        """
        Cerrar conexión con MySQL
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("✓ Conexión cerrada")
    
    def create_table(self):
        """
        Crear tablas en MySQL si no existen:
        - movies
        - genre_stats (conteo de películas por género)
        - director_stats (conteo de películas por director)
        """
        logger.info("📊 Creando/verificando tablas en MySQL...")
        
        create_movies_table = """
        CREATE TABLE IF NOT EXISTS movies (
            id INT PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            genre VARCHAR(200),
            director TEXT,
            overview TEXT,
            release_date DATE,
            popularity DECIMAL(10,3),
            vote_average DECIMAL(4,2),
            vote_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_title (title(100)),
            INDEX idx_genre (genre),
            INDEX idx_director (director(100)),
            INDEX idx_release_date (release_date),
            INDEX idx_popularity (popularity),
            INDEX idx_vote_average (vote_average)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """

        create_genre_stats = """
        CREATE TABLE IF NOT EXISTS genre_stats (
            genre VARCHAR(200) PRIMARY KEY,
            movie_count INT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """

        create_director_stats = """
        CREATE TABLE IF NOT EXISTS director_stats (
            director VARCHAR(300) PRIMARY KEY,
            movie_count INT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_movies_table)
            cursor.execute(create_genre_stats)
            cursor.execute(create_director_stats)
            self.connection.commit()
            logger.info("  ✓ Tablas 'movies', 'genre_stats' y 'director_stats' listas")
            cursor.close()
            return True
        except Error as e:
            logger.error(f"❌ Error creando tablas: {e}")
            return False
    
    def extract(self):
        """
        EXTRACT: Cargar los datos de ambos CSV
        """
        logger.info("📥 EXTRAYENDO datos...")
        
        try:
            # Cargar top_rated_movies.csv
            self.df_top_rated = pd.read_csv(
                self.top_rated_path,
                encoding='utf-8',
                on_bad_lines='skip'
            )
            logger.info(f"  ✓ Top rated movies: {len(self.df_top_rated)} registros")
            
            # Cargar movies_id_genre_director.csv
            self.df_genre_director = pd.read_csv(
                self.id_genre_director_path,
                encoding='utf-8',
                on_bad_lines='skip'
            )
            logger.info(f"  ✓ Genre/Director data: {len(self.df_genre_director)} registros")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en extracción: {e}")
            return False
    
    def transform(self):
        """
        TRANSFORM: Limpiar, validar y estandarizar datos
        """
        logger.info("🔄 TRANSFORMANDO datos...")
        
        # 1. Limpiar nombres de columnas
        self.df_top_rated.columns = self.df_top_rated.columns.str.strip()
        self.df_genre_director.columns = self.df_genre_director.columns.str.strip()
        
        # 2. Convertir tipos de datos
        self.df_top_rated['id'] = pd.to_numeric(self.df_top_rated['id'], errors='coerce')
        self.df_genre_director['id'] = pd.to_numeric(self.df_genre_director['id'], errors='coerce')
        
        # 3. Limpiar y validar release_date
        self.df_top_rated['release_date'] = pd.to_datetime(
            self.df_top_rated['release_date'], 
            errors='coerce'
        )
        
        # 4. Validar campos numéricos
        self.df_top_rated['popularity'] = pd.to_numeric(
            self.df_top_rated['popularity'], 
            errors='coerce'
        )
        self.df_top_rated['vote_average'] = pd.to_numeric(
            self.df_top_rated['vote_average'], 
            errors='coerce'
        )
        self.df_top_rated['vote_count'] = pd.to_numeric(
            self.df_top_rated['vote_count'], 
            errors='coerce'
        )
        
        # 5. Limpiar campos de texto
        text_columns = ['title', 'overview', 'genre', 'director']
        
        for col in text_columns:
            if col in self.df_top_rated.columns:
                self.df_top_rated[col] = self.df_top_rated[col].astype(str).str.strip()
                self.df_top_rated[col] = self.df_top_rated[col].replace(['nan', 'None', ''], None)
                
            if col in self.df_genre_director.columns:
                self.df_genre_director[col] = self.df_genre_director[col].astype(str).str.strip()
                self.df_genre_director[col] = self.df_genre_director[col].replace(['nan', 'None', ''], None)
        
        # 6. Eliminar duplicados
        self.df_top_rated = self.df_top_rated.drop_duplicates(subset=['id'], keep='first')
        self.df_genre_director = self.df_genre_director.drop_duplicates(subset=['id'], keep='first')
        
        # 7. Eliminar registros con ID nulo
        self.df_top_rated = self.df_top_rated.dropna(subset=['id'])
        self.df_genre_director = self.df_genre_director.dropna(subset=['id'])
        
        logger.info("  ✓ Datos transformados correctamente")
        
        return True
    
    def load_unify(self):
        """
        Unificar ambos DataFrames
        """
        logger.info("🔗 UNIFICANDO datasets...")
        
        # Merge de ambos DataFrames
        self.df_unified = pd.merge(
            self.df_top_rated,
            self.df_genre_director,
            on='id',
            how='left'
        )
        
        # Reordenar columnas
        column_order = [
            'id', 'title', 'genre', 'director', 'overview',
            'release_date', 'popularity', 'vote_average', 'vote_count'
        ]
        
        self.df_unified = self.df_unified[column_order]
        
        # Convertir NaT a None para MySQL
        self.df_unified['release_date'] = self.df_unified['release_date'].where(
            pd.notna(self.df_unified['release_date']), None
        )
        
        logger.info(f"  ✓ Dataset unificado: {len(self.df_unified)} registros")
        
        return True

    def build_genre_director_stats(self):
        """
        Construir tablas de conteo por género y por director
        a partir de df_unified.
        
        Importante: si una película tiene varios géneros o directores,
        se cuenta 1 vez para cada género/director individual.
        """
        logger.info("📈 Construyendo estadísticas por género y director...")

        # --- GÉNEROS ---
        df_genres = self.df_unified[['id', 'genre']].dropna().copy()
        df_genres['genre'] = df_genres['genre'].str.split(',')
        df_genres = df_genres.explode('genre')
        df_genres['genre'] = df_genres['genre'].str.strip()
        df_genres = df_genres[df_genres['genre'] != '']
        self.genre_stats = (
            df_genres['genre']
            .value_counts()
            .reset_index()
            .rename(columns={'index': 'genre', 'genre': 'movie_count'})
        )

        # --- DIRECTORES ---
        df_directors = self.df_unified[['id', 'director']].dropna().copy()
        df_directors['director'] = df_directors['director'].str.split(',')
        df_directors = df_directors.explode('director')
        df_directors['director'] = df_directors['director'].str.strip()
        df_directors = df_directors[df_directors['director'] != '']
        self.director_stats = (
            df_directors['director']
            .value_counts()
            .reset_index()
            .rename(columns={'index': 'director', 'director': 'movie_count'})
        )

        logger.info(f"  ✓ {len(self.genre_stats)} géneros y {len(self.director_stats)} directores contabilizados")
        return True
    
    def load_to_mysql(self, batch_size=1000, truncate=False):
        """
        LOAD: Cargar datos de películas a MySQL usando UPSERT (INSERT ON DUPLICATE KEY UPDATE)
        """
        logger.info("📤 CARGANDO datos de películas a MySQL...")
        
        # Como ya dropeamos y recreamos la DB entera, truncate es redundante,
        # pero lo dejamos por compatibilidad.
        if truncate:
            logger.info("  ⚠ Truncando tabla 'movies' antes de cargar...")
            cursor = self.connection.cursor()
            cursor.execute("TRUNCATE TABLE movies")
            self.connection.commit()
            cursor.close()
        
        insert_query = """
        INSERT INTO movies 
            (id, title, genre, director, overview, release_date, 
             popularity, vote_average, vote_count)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            genre = VALUES(genre),
            director = VALUES(director),
            overview = VALUES(overview),
            release_date = VALUES(release_date),
            popularity = VALUES(popularity),
            vote_average = VALUES(vote_average),
            vote_count = VALUES(vote_count)
        """
        
        cursor = self.connection.cursor()
        total_rows = len(self.df_unified)
        inserted = 0
        errors = 0
        
        try:
            # Cargar en lotes
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch = self.df_unified.iloc[start_idx:end_idx]
                
                batch_data = []
                for _, row in batch.iterrows():
                    # Truncar director para respetar límites en caso de cambio de schema
                    director_val = row['director']
                    if isinstance(director_val, str):
                        director_val = director_val.strip()
                        if len(director_val) > 300:
                            director_val = director_val[:300]

                    batch_data.append((
                        int(row['id']),
                        row['title'],
                        row['genre'],
                        director_val,
                        row['overview'],
                        row['release_date'],
                        float(row['popularity']) if pd.notna(row['popularity']) else None,
                        float(row['vote_average']) if pd.notna(row['vote_average']) else None,
                        int(row['vote_count']) if pd.notna(row['vote_count']) else None
                    ))
                
                try:
                    cursor.executemany(insert_query, batch_data)
                    self.connection.commit()
                    inserted += len(batch_data)
                    logger.info(f"  ✓ Batch {start_idx//batch_size + 1}: {inserted}/{total_rows} registros")
                except Error as e:
                    errors += len(batch_data)
                    logger.error(f"  ❌ Error en batch {start_idx//batch_size + 1}: {e}")
                    self.connection.rollback()
            
            cursor.close()
            logger.info(f"\n  ✅ Carga de películas completada: {inserted} registros exitosos, {errors} errores")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error general en carga de películas: {e}")
            cursor.close()
            return False

    def load_stats_to_mysql(self, truncate=False):
        """
        Cargar las estadísticas por género y director a MySQL.
        """
        logger.info("📤 CARGANDO estadísticas de género y director a MySQL...")

        cursor = self.connection.cursor()

        # Como la DB es nueva cada vez, TRUNCATE también es redundante,
        # pero lo dejamos por si se cambia la política a futuro.
        if truncate:
            logger.info("  ⚠ Truncando tablas 'genre_stats' y 'director_stats' antes de cargar...")
            cursor.execute("TRUNCATE TABLE genre_stats")
            cursor.execute("TRUNCATE TABLE director_stats")
            self.connection.commit()

        insert_genre = """
            INSERT INTO genre_stats (genre, movie_count)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                movie_count = VALUES(movie_count),
                updated_at = CURRENT_TIMESTAMP;
        """

        insert_director = """
            INSERT INTO director_stats (director, movie_count)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                movie_count = VALUES(movie_count),
                updated_at = CURRENT_TIMESTAMP;
        """

        genre_data = list(self.genre_stats.itertuples(index=False, name=None))
        cursor.executemany(insert_genre, genre_data)

        director_data = list(self.director_stats.itertuples(index=False, name=None))
        cursor.executemany(insert_director, director_data)

        self.connection.commit()
        cursor.close()
        logger.info("  ✅ Estadísticas de género y director cargadas correctamente")
        return True
    
    def validate_load(self):
        """
        Validar que los datos se cargaron correctamente (tabla movies)
        """
        logger.info("\n✅ VALIDANDO carga en MySQL (tabla movies)...")
        
        cursor = self.connection.cursor(dictionary=True)
        
        cursor.execute("SELECT COUNT(*) as total FROM movies")
        total = cursor.fetchone()['total']
        logger.info(f"  📊 Total de registros en MySQL: {total}")
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(genre) as with_genre,
                COUNT(director) as with_director,
                COUNT(release_date) as with_date,
                MIN(release_date) as min_date,
                MAX(release_date) as max_date,
                AVG(vote_average) as avg_rating,
                AVG(popularity) as avg_popularity
            FROM movies
        """)
        
        stats = cursor.fetchone()
        logger.info(f"  • Películas con género (no nulo): {stats['with_genre']}")
        logger.info(f"  • Películas con director (no nulo): {stats['with_director']}")
        logger.info(f"  • Películas con fecha: {stats['with_date']}")
        if stats['min_date'] and stats['max_date']:
            logger.info(f"  • Rango de fechas: {stats['min_date']} a {stats['max_date']}")
        logger.info(f"  • Rating promedio: {stats['avg_rating']:.2f}" if stats['avg_rating'] is not None else "  • Rating promedio: N/A")
        logger.info(f"  • Popularidad promedio: {stats['avg_popularity']:.2f}" if stats['avg_popularity'] is not None else "  • Popularidad promedio: N/A")
        
        cursor.execute("SELECT id, title, genre, director, vote_average FROM movies LIMIT 5")
        movies = cursor.fetchall()
        logger.info("\n  📋 Muestra de datos cargados:")
        for movie in movies:
            logger.info(f"     • {movie['title']} - Rating: {movie['vote_average']}")
        
        cursor.close()
        return True
    
    def run(self, batch_size=1000, truncate=False):
        """
        Ejecutar todo el proceso ETL:
        - Dropear + crear DB
        - Crear tablas
        - Extraer
        - Transformar
        - Unificar
        - Cargar películas
        - Construir y cargar estadísticas por género y director
        - Validar
        """
        logger.info("🚀 INICIANDO PROCESO ETL - MOVIES TO MYSQL")
        logger.info("="*70)
        
        try:
            if not self.connect_to_db():
                return False
            
            if not self.create_table():
                return False
            
            if not self.extract():
                return False
            
            if not self.transform():
                return False
            
            if not self.load_unify():
                return False
            
            if not self.load_to_mysql(batch_size=batch_size, truncate=truncate):
                return False

            self.build_genre_director_stats()
            self.load_stats_to_mysql(truncate=truncate)
            
            self.validate_load()
            
            logger.info("\n" + "="*70)
            logger.info("✨ PROCESO ETL COMPLETADO EXITOSAMENTE")
            logger.info("="*70)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en proceso ETL: {e}")
            return False
        
        finally:
            self.close_connection()


# EJEMPLO DE USO
if __name__ == "__main__":
    
    db_config = {
        'host': 'localhost',
        'database': 'movies_dw',    # se dropea y recrea en cada run
        'user': 'root',
        'password': '123456',       # tu contraseña real de MySQL
        'port': 3306
    }
    
    etl = MoviesETL(
        top_rated_path='top_rated_movies.csv',
        id_genre_director_path='movies_id_genre_director_.csv',
        db_config=db_config
    )
    
    # Ya que la DB se recrea siempre, no hace falta truncate=True
    success = etl.run(batch_size=1000, truncate=False)
    
    if success:
        print("\n🎉 ¡Datos cargados exitosamente al Data Warehouse (DB limpia en cada run)!")
    else:
        print("\n❌ Hubo errores en el proceso ETL")
