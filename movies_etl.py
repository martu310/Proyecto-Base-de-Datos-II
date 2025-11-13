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
    
    def connect_to_db(self):
        """
        Establecer conexión con MySQL
        """
        if mysql is None:
            logger.error("mysql.connector no está disponible; instala el paquete con: pip install mysql-connector-python")
            return False

        try:
            self.connection = mysql.connector.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config.get('port', 3306)
            )
            
            if self.connection.is_connected():
                logger.info("Conexión exitosa a MySQL")
                return True
        except Error as e:
            logger.error(f"Error conectando a MySQL: {e}")
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
        Crear tabla en MySQL si no existe
        """
        logger.info("Creando/verificando tabla en MySQL...")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS movies (
            id INT PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            genre VARCHAR(200),
            director VARCHAR(300),
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
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_table_query)
            self.connection.commit()
            logger.info("Tabla 'movies' lista")
            cursor.close()
            return True
        except Error as e:
            logger.error(f"Error creando tabla: {e}")
            return False
    
    def extract(self):
        """
        EXTRACT: Cargar los datos de ambos CSV
        """
        logger.info("EXTRAYENDO datos...")
        
        try:
            # Cargar top_rated_movies.csv
            self.df_top_rated = pd.read_csv(
                self.top_rated_path,
                encoding='utf-8',
                on_bad_lines='skip'
            )
            logger.info(f"Top rated movies: {len(self.df_top_rated)} registros")
            
            # Cargar movies_id_genre_director.csv
            self.df_genre_director = pd.read_csv(
                self.id_genre_director_path,
                encoding='utf-8',
                on_bad_lines='skip'
            )
            logger.info(f"Genre/Director data: {len(self.df_genre_director)} registros")
            
            return True
            
        except Exception as e:
            logger.error(f"Error en extracción: {e}")
            return False
    
    def transform(self):
        """
        TRANSFORM: Limpiar, validar y estandarizar datos
        """
        logger.info("TRANSFORMANDO datos...")
        
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
        
        logger.info("Datos transformados correctamente")
        
        return True
    
    def load_unify(self):
        """
        Unificar ambos DataFrames
        """
        logger.info("UNIFICANDO datasets...")
        
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
        
        logger.info(f"Dataset unificado: {len(self.df_unified)} registros")
        
        return True
    
    def load_to_mysql(self, batch_size=1000, truncate=False):
        """
        LOAD: Cargar datos a MySQL usando UPSERT (INSERT ON DUPLICATE KEY UPDATE)
        """
        logger.info("CARGANDO datos a MySQL...")
        
        if truncate:
            logger.info("Truncando tabla antes de cargar...")
            cursor = self.connection.cursor()
            cursor.execute("TRUNCATE TABLE movies")
            self.connection.commit()
            cursor.close()
        
        # Query de INSERT con ON DUPLICATE KEY UPDATE
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
                
                # Preparar datos del batch
                batch_data = []
                for _, row in batch.iterrows():
                    batch_data.append((
                        int(row['id']),
                        row['title'],
                        row['genre'],
                        row['director'],
                        row['overview'],
                        row['release_date'],
                        float(row['popularity']) if pd.notna(row['popularity']) else None,
                        float(row['vote_average']) if pd.notna(row['vote_average']) else None,
                        int(row['vote_count']) if pd.notna(row['vote_count']) else None
                    ))
                
                # Ejecutar batch
                try:
                    cursor.executemany(insert_query, batch_data)
                    self.connection.commit()
                    inserted += len(batch_data)
                    logger.info(f"Batch {start_idx//batch_size + 1}: {inserted}/{total_rows} registros")
                except Error as e:
                    errors += len(batch_data)
                    logger.error(f"Error en batch {start_idx//batch_size + 1}: {e}")
                    self.connection.rollback()
            
            cursor.close()
            logger.info(f"\n Carga completada: {inserted} registros exitosos, {errors} errores")
            
            return True
            
        except Exception as e:
            logger.error(f"Error general en carga: {e}")
            cursor.close()
            return False
    
    def validate_load(self):
        """
        Validar que los datos se cargaron correctamente
        """
        logger.info("\n VALIDANDO carga en MySQL...")
        
        cursor = self.connection.cursor(dictionary=True)
        
        # Contar registros totales
        cursor.execute("SELECT COUNT(*) as total FROM movies")
        total = cursor.fetchone()['total']
        logger.info(f" Total de registros en MySQL: {total}")
        
        # Estadísticas básicas
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
        logger.info(f"  • Películas con género: {stats['with_genre']}")
        logger.info(f"  • Películas con director: {stats['with_director']}")
        logger.info(f"  • Películas con fecha: {stats['with_date']}")
        if stats['min_date'] and stats['max_date']:
            logger.info(f"  • Rango de fechas: {stats['min_date']} a {stats['max_date']}")
        logger.info(f"  • Rating promedio: {stats['avg_rating']:.2f}")
        logger.info(f"  • Popularidad promedio: {stats['avg_popularity']:.2f}")
        
        # Mostrar algunas películas de ejemplo
        cursor.execute("SELECT id, title, genre, director, vote_average FROM movies LIMIT 5")
        movies = cursor.fetchall()
        logger.info("\n  📋 Muestra de datos cargados:")
        for movie in movies:
            logger.info(f"     • {movie['title']} - Rating: {movie['vote_average']}")
        
        cursor.close()
        return True
    
    def run(self, batch_size=1000, truncate=False):
        """
        Ejecutar todo el proceso ETL
        """
        logger.info("🚀 INICIANDO PROCESO ETL - MOVIES TO MYSQL")
        logger.info("="*70)
        
        try:
            # Conectar a MySQL
            if not self.connect_to_db():
                return False
            
            # Crear tabla
            if not self.create_table():
                return False
            
            # Extraer
            if not self.extract():
                return False
            
            # Transformar
            if not self.transform():
                return False
            
            # Unificar
            if not self.load_unify():
                return False
            
            # Cargar a MySQL
            if not self.load_to_mysql(batch_size=batch_size, truncate=truncate):
                return False
            
            # Validar
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
    
    # Configuración de la base de datos
    db_config = {
        'host': 'localhost',        # o la IP de tu servidor MySQL
        'database': 'movies_dw',    # nombre de tu data warehouse
        'user': 'root',       # tu usuario MySQL
        'password': 'Agustina06',  # tu contraseña MySQL
        'port': 3306                # puerto MySQL (por defecto 3306)
    }
    
    # Inicializar ETL
    etl = MoviesETL(
        top_rated_path='C:\\Users\\agust\\OneDrive\\Documentos\\GitHub\\Proyecto-Base-de-Datos-II\\top_rated_movies.csv',
        id_genre_director_path='C:\\Users\\agust\\OneDrive\\Documentos\\GitHub\\Proyecto-Base-de-Datos-II\\movies_id_genre_director_.csv',
        db_config=db_config
    )
    
    # Ejecutar proceso completo
    # truncate=True eliminará todos los datos antes de cargar
    # truncate=False hará UPSERT (actualizar existentes, insertar nuevos)
    success = etl.run(batch_size=1000, truncate=False)
    
    if success:
        print("\n🎉 ¡Datos cargados exitosamente al Data Warehouse!")
    else:
        print("\n❌ Hubo errores en el proceso ETL")