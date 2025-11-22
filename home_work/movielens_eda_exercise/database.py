from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# # connection string for sqlite (requirements: SQLAlchemy)
# SQLALCHEMY_DATABASE_URL = "sqlite:///movielens.db"
# engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={'check_same_thread': False})

# # connection string for PostGres (requirements: psycopg2-binary)
# SQLALCHEMY_DATABASE_URL = "postgresql://postgres:postgres@localhost/TodoApplicationDatabase"
# engine = create_engine(SQLALCHEMY_DATABASE_URL)

# connection string for MySql (requirements: PyMySQL)
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:AIdev2025@127.0.0.1:3306/movielens_llm"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={
        'connect_timeout': 1800  # Connection timeout in seconds (30 minutes)
    },
    pool_pre_ping=True,  # Test connections before using them
    pool_recycle=3600,   # Recycle connections after 1 hour
    pool_size=10,        # Number of connections to keep in pool
    max_overflow=20      # Additional connections beyond pool_size
)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
