# arbcore/database/adapters/__init__.py
# 数据库适配器 - PostgreSQL 专用

from .base import IDatabaseAdapter
from .postgres_adapter import PostgresAdapter

__all__ = ['IDatabaseAdapter', 'PostgresAdapter']
