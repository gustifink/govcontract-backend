"""
Database connection and session management
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from config import get_settings
import os

settings = get_settings()

# Use SQLite for local dev if DATABASE_URL contains 'sqlite'
# Otherwise use PostgreSQL
db_url = settings.database_url

# Ensure SQLite URLs use aiosqlite driver for async support
if 'sqlite' in db_url and '+aiosqlite' not in db_url:
    db_url = db_url.replace('sqlite:', 'sqlite+aiosqlite:')

# Check if using SQLite (for easy local dev)
if 'sqlite' in db_url:
    engine = create_async_engine(
        db_url,
        echo=settings.debug,
    )
else:
    engine = create_async_engine(
        db_url,
        echo=settings.debug,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)


class Base(DeclarativeBase):
    pass


async def get_db():
    """Dependency for FastAPI routes"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Create all tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

