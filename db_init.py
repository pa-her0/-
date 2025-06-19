import os
import sqlite3
import time
from rich.console import Console
from rich.prompt import Confirm

console = Console()

def initialize_database():
    """初始化数据库（清空所有表）"""
    db_file = "project2025.db"
    
    try:
        # 创建或打开数据库连接
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # 检查数据库中的表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = cursor.fetchall()
        
        if existing_tables:
            if not Confirm.ask("[yellow]数据库中存在表，是否要清除所有现有的表和数据？[/yellow]"):
                console.print("[blue]保持数据库结构不变[/blue]")
                cursor.close()
                conn.close()
                return True
            
            try:
                # 禁用外键约束
                cursor.execute("PRAGMA foreign_keys = OFF;")
                
                # 开始事务
                cursor.execute("BEGIN TRANSACTION;")
                
                # 获取所有表名
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                # 删除所有表
                for table in tables:
                    table_name = table[0]
                    try:
                        # 使用双引号包裹表名，防止特殊字符
                        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
                        console.print(f"[green]✓[/green] 删除表 {table_name}")
                    except sqlite3.Error as e:
                        console.print(f"[yellow]警告: 删除表 {table_name} 时出现问题: {str(e)}[/yellow]")
                
                # 提交事务
                conn.commit()
                
                # 重新启用外键约束
                cursor.execute("PRAGMA foreign_keys = ON;")
                
                # 执行VACUUM来清理数据库文件
                cursor.execute("VACUUM;")
                
                console.print("[green]✓[/green] 已清空数据库")
            except sqlite3.Error as e:
                # 如果出错，回滚事务
                conn.rollback()
                console.print(f"[red]错误: 清空数据库时出现问题: {str(e)}[/red]")
                cursor.close()
                conn.close()
                return False
        else:
            console.print("[blue]数据库已是空的[/blue]")
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        
        console.print("[green]✓[/green] 数据库初始化完成")
        return True
        
    except sqlite3.Error as e:
        console.print(f"[red]错误: 数据库操作失败: {str(e)}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]错误: {str(e)}[/red]")
        return False