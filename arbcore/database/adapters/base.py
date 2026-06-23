# arbcore/database/adapters/base.py
# 数据库适配器接口 - PostgreSQL 专用

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class IDatabaseAdapter(ABC):
    """
    数据库适配器接口
    
    极简设计，只保留核心操作：
    - 连接管理
    - 事务控制
    - execute/fetch 统一接口
    """
    
    @abstractmethod
    def connect(self) -> bool:
        """建立数据库连接"""
        pass
    
    @abstractmethod
    def close(self):
        """关闭数据库连接"""
        pass
    
    @abstractmethod
    def get_connection(self):
        """获取数据库连接对象"""
        pass
    
    @abstractmethod
    def execute(self, sql: str, params: Tuple = None) -> Any:
        """执行 SQL 语句"""
        pass
    
    @abstractmethod
    def executemany(self, sql: str, params_list: List[Tuple]) -> Any:
        """批量执行 SQL 语句"""
        pass
    
    @abstractmethod
    def fetchone(self, sql: str, params: Tuple = None) -> Optional[Tuple]:
        """执行查询并返回一行"""
        pass
    
    @abstractmethod
    def fetchall(self, sql: str, params: Tuple = None) -> List[Tuple]:
        """执行查询并返回所有行"""
        pass
    
    @abstractmethod
    def commit(self):
        """提交事务"""
        pass
    
    @abstractmethod
    def rollback(self):
        """回滚事务"""
        pass
    
    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        pass
