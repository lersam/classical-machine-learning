from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# connection string for sqlite (requirements: SQLAlchemy)
SQLALCHEMY_DATABASE_URL = "sqlite://./movielens.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={'check_same_thread': False})

# # connection string for PostGres (requirements: psycopg2-binary)
# SQLALCHEMY_DATABASE_URL = "postgresql://postgres:postgres@localhost/TodoApplicationDatabase"
# engine = create_engine(SQLALCHEMY_DATABASE_URL)

# # connection string for MySql (requirements: PyMySQL)
# SQLALCHEMY_DATABASE_URL = "mysql+pymysql://admin:samuel@127.0.0.1:3306/TodoApplication"
# engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
