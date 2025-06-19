import csv
import json
import sqlite3
from rich.console import Console
from rich.panel import Panel

class DataExporter:
    def __init__(self, db_name="project2025"):
        self.db_file = f"{db_name}.db"
        self.console = Console()

    def _export_to_csv(self, data, column_names, filename):
        """将数据写入CSV文件"""
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(column_names)  # 写入表头
                writer.writerows(data)         # 写入数据
            return True
        except IOError as e:
            self.console.print(Panel(f"写入CSV文件失败: {e}", title="[bold red]错误[/bold red]", border_style="red"))
            return False

    def _export_to_json(self, data, column_names, filename):
        """将数据写入JSON文件"""
        try:
            # 将每行数据转换为字典
            records = [dict(zip(column_names, row)) for row in data]
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            return True
        except IOError as e:
            self.console.print(Panel(f"写入JSON文件失败: {e}", title="[bold red]错误[/bold red]", border_style="red"))
            return False

    def export_query_to_file(self, sql_query: str, filename: str):
        """执行查询并将结果导出到文件"""
        file_ext = filename.lower().split('.')[-1]
        if file_ext not in ['csv', 'json']:
            self.console.print(Panel(f"不支持的文件格式: .{file_ext}。请使用 .csv 或 .json。", title="[bold red]导出错误[/bold red]", border_style="red"))
            return

        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            self.console.print(f"正在执行查询以导出数据...")
            cursor.execute(sql_query)
            
            results = cursor.fetchall()
            if not results:
                self.console.print(Panel("查询没有返回任何数据，未创建文件。", title="[bold yellow]提示[/bold yellow]", border_style="yellow"))
                return

            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            success = False
            if file_ext == 'csv':
                success = self._export_to_csv(results, column_names, filename)
            elif file_ext == 'json':
                success = self._export_to_json(results, column_names, filename)

            if success:
                self.console.print(Panel(f"成功将 {len(results)} 条记录导出到 [bold cyan]{filename}[/bold cyan]", title="[bold green]导出完成[/bold green]", border_style="green"))

        except sqlite3.Error as e:
            self.console.print(Panel(f"数据库查询失败: {e}", title="[bold red]错误[/bold red]", border_style="red"))
        finally:
            if conn:
                conn.close()