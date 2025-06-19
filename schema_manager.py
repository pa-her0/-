import sqlite3
import json
import os
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich import box
from rich.tree import Tree
import re

def is_valid_identifier(identifier):
    if not identifier or not isinstance(identifier, str):
        return False
    if not re.fullmatch(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
        return False
    return True

def is_safe_type_definition(type_def):
    if not type_def or not isinstance(type_def, str):
        return False
    if ';' in type_def or '--' in type_def or '/*' in type_def or '*/' in type_def:
        return False
    return True

class SchemaManager:
    def __init__(self, db_name="project2025"):
        self.db_name = db_name
        self.db_file = f"{db_name}.db"
        self.console = Console(force_terminal=True) # 保持强制颜色输出

    # ... 其他方法（create_table, import_schema_from_json 等）保持不变 ...
    def create_table(self, table_name, columns):
        if not is_valid_identifier(table_name):
            self.console.print(Panel(f"创建表失败: 无效的表名 '{table_name}'", title="[bold red]错误[/bold red]", border_style="red"))
            return False
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            column_defs = []
            constraints = []
            for col in columns:
                col_name = col.get('name')
                col_type = col.get('type')
                if not col_name or not col_type:
                    self.console.print(Panel(f"表 {table_name} 中列定义不完整。", title="[bold red]错误[/bold red]", border_style="red"))
                    return False
                if col_name == 'PRIMARY KEY':
                    if not (re.fullmatch(r"^\([\w\s,]+\)$", col_type) or is_valid_identifier(col_type)):
                       self.console.print(Panel(f"表 {table_name} 中 PRIMARY KEY 类型定义不安全: {col_type}", title="[bold red]错误[/bold red]", border_style="red"))
                       return False
                    constraints.append(f"PRIMARY KEY {col_type}")
                    continue
                elif col_name == 'FOREIGN KEY':
                    if not re.fullmatch(r"^\([\w\s,]+\)\s+REFERENCES\s+\w+\([\w\s,]+\)$", col_type, re.IGNORECASE):
                       self.console.print(Panel(f"表 {table_name} 中 FOREIGN KEY 类型定义不安全: {col_type}", title="[bold red]错误[/bold red]", border_style="red"))
                       return False
                    constraints.append(f"FOREIGN KEY {col_type}")
                    continue
                if not is_valid_identifier(col_name):
                    self.console.print(Panel(f"表 {table_name} 中无效的列名: {col_name}", title="[bold red]错误[/bold red]", border_style="red"))
                    return False
                if not is_safe_type_definition(col_type):
                    self.console.print(Panel(f"表 {table_name}, 列 {col_name} 中不安全的类型定义: {col_type}", title="[bold red]错误[/bold red]", border_style="red"))
                    return False
                col_def = f"\"{col_name}\" {col_type}"
                column_defs.append(col_def)
            all_defs = column_defs + constraints
            create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(all_defs)})"
            sql_syntax = Syntax(create_table_sql, "sql", theme="monokai", line_numbers=True)
            self.console.print(Panel(sql_syntax, title=f"[bold blue]创建表 {table_name}[/bold blue]", border_style="blue"))
            cursor.execute(create_table_sql)
            conn.commit()
            self.console.print(Panel(f"表 [bold cyan]{table_name}[/bold cyan] 创建成功！", border_style="green"))
            return True
        except sqlite3.Error as e:
            self.console.print(Panel(f"创建表 {table_name} 失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            return False
        finally:
            if conn:
                conn.close()

    def import_schema_from_json(self, json_file):
        try:
            if not os.path.exists(json_file):
                self.console.print(Panel(f"文件 {json_file} 不存在", title="[bold red]错误[/bold red]", border_style="red"))
                return False
            with open(json_file, 'r', encoding='utf-8') as f:
                schema_data = json.load(f)
            schema_json_display = json.dumps(schema_data, indent=2, ensure_ascii=False)
            json_syntax = Syntax(schema_json_display, "json", theme="monokai")
            self.console.print(Panel(json_syntax, title="[bold blue]导入的数据库模式[/bold blue]", border_style="blue"))
            schema_tree = Tree("[bold]数据库模式[/bold]")
            for table_info in schema_data:
                table_name = table_info['table_name']
                columns = table_info['columns']
                table_node = schema_tree.add(f"[bold cyan]{table_name}[/bold cyan]")
                for col in columns:
                    col_text = f"{col['name']} ({col['type']})"
                    table_node.add(col_text)
                if not self.create_table(table_name, columns):
                    self.console.print(Panel(f"为表 {table_name} 创建失败，中止导入。", title="[bold red]导入错误[/bold red]", border_style="red"))
                    return False 
            self.console.print(Panel(schema_tree, title="[bold green]导入的数据库模式结构[/bold green]", border_style="green"))
            return True
        except json.JSONDecodeError:
            self.console.print(Panel(f"JSON文件 {json_file} 格式错误", title="[bold red]错误[/bold red]", border_style="red"))
            return False
        except Exception as e:
            self.console.print(Panel(f"导入模式失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            return False

    def reset_schema(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()
            if not tables:
                self.console.print(Panel("数据库中没有表需要重置", title="[bold yellow]提示[/bold yellow]", border_style="yellow"))
                return True
            table_list_display = Table(title="将要删除的表", box=box.ROUNDED, header_style="bold magenta")
            table_list_display.add_column("表名", style="cyan")
            for table in tables:
                table_list_display.add_row(table[0])
            self.console.print(table_list_display, justify="center")
            for table in tables:
                table_name = table[0]
                if not is_valid_identifier(table_name):
                    self.console.print(Panel(f"跳过删除无效的表名: {table_name}", title="[bold red]错误[/bold red]", border_style="red"))
                    continue
                drop_sql = f"DROP TABLE IF EXISTS \"{table_name}\""
                cursor.execute(drop_sql)
            conn.commit()
            self.console.print(Panel("[bold green]✓[/bold green] 数据库模式已重置", border_style="green"))
            return True
        except sqlite3.Error as e:
            self.console.print(Panel(f"重置模式失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            return False
        finally:
            if conn:
                conn.close()
                
    def show_schema(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()
            if not tables:
                self.console.print(Panel("数据库中没有表", title="[bold yellow]提示[/bold yellow]", border_style="yellow", justify="center"))
                return True
            
            schema_tree_display = Tree("[bold]数据库模式[/bold]", guide_style="cyan")
            for table in tables:
                table_name = table[0]
                cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
                columns = cursor.fetchall()
                table_node = schema_tree_display.add(f"[bold cyan]{table_name}[/bold cyan]")
                col_names = [f"{col[1]} [dim]({col[2]})[/dim]" for col in columns]
                table_node.add(", ".join(col_names))
            
            self.console.print(Panel(schema_tree_display, title="[bold green]当前数据库模式[/bold green]", border_style="green"))
            return True
        except sqlite3.Error as e:
            self.console.print(Panel(f"显示模式失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            return False
        finally:
            if conn:
                conn.close()

    def show_table_details(self, table_name):
        """显示单个表的详细信息"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                self.console.print(Panel(f"表 [bold red]{table_name}[/bold red] 不存在。", title="[bold yellow]提示[/bold yellow]", border_style="yellow"))
                return

            cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
            columns = cursor.fetchall()
            cursor.execute(f"PRAGMA foreign_key_list(\"{table_name}\")")
            foreign_keys = {fk[3]: f"{fk[2]}({fk[4]})" for fk in cursor.fetchall()}

            table = Table(title=f"表 [bold cyan]{table_name}[/bold cyan] 的详细结构", box=box.ROUNDED, show_lines=True)
            table.add_column("列名", style="cyan")
            table.add_column("数据类型", style="green")
            table.add_column("非空", style="magenta", justify="center")
            table.add_column("主键", style="yellow", justify="center")
            table.add_column("外键", style="blue")

            for col in columns:
                # 【修正点】正确地从元组中解析列信息
                # (cid, name, type, notnull, dflt_value, pk)
                name, type, not_null, pk = col[1], col[2], col[3], col[5]
                
                is_pk = "✓" if pk > 0 else ""
                is_not_null = "✓" if not_null else ""
                fk_info = foreign_keys.get(name, "")
                table.add_row(name, str(type), is_not_null, is_pk, fk_info)

            self.console.print(table)
        except sqlite3.Error as e:
            self.console.print(Panel(f"获取表信息失败: {e}", title="[bold red]错误[/bold red]", border_style="red"))
        finally:
            if conn:
                conn.close()

    def import_data_from_json(self, json_file):
        # ... (此方法保持不变) ...
        try:
            if not os.path.exists(json_file):
                self.console.print(Panel(f"文件 {json_file} 不存在", title="[bold red]错误[/bold red]", border_style="red"))
                return False
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            conn = None
            try:
                conn = sqlite3.connect(self.db_file)
                cursor = conn.cursor()
                success_count = 0
                total_tables = len(data)
                tables_status = {}
                for table_name, records in data.items():
                    if not records:
                        continue
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    if not cursor.fetchone():
                        self.console.print(Panel(f"表 {table_name} 不存在。正在尝试自动创建...", title="[bold yellow]提示[/bold yellow]", border_style="yellow"))
                        columns = []
                        first_record = records[0]
                        for col_name, value in first_record.items():
                            if isinstance(value, int): col_type = "INTEGER"
                            elif isinstance(value, float): col_type = "REAL"
                            else: col_type = "TEXT"
                            columns.append(f'"{col_name}" {col_type}')
                        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns)})'
                        try:
                            cursor.execute(create_sql)
                            self.console.print(f"[bold green]✓[/bold green] 自动创建表 {table_name}")
                        except sqlite3.Error as e:
                            self.console.print(Panel(f"自动创建表失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
                            tables_status[table_name] = False
                            continue
                    columns = list(records[0].keys())
                    placeholders = ','.join(['?' for _ in columns])
                    column_names = ','.join([f'"{col}"' for col in columns])
                    insert_sql = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
                    sql_syntax = Syntax(insert_sql, "sql", theme="monokai", line_numbers=True)
                    self.console.print(Panel(sql_syntax, title=f"[bold blue]导入数据到 {table_name}[/bold blue]", border_style="blue"))
                    success_inserts = 0
                    for record in records:
                        values = [record[col] for col in columns]
                        try:
                            cursor.execute(insert_sql, values)
                            success_inserts += 1
                        except sqlite3.Error as e:
                            self.console.print(Panel(f"插入数据失败: {str(e)}\n表: {table_name}\n数据: {record}", title="[bold red]错误[/bold red]", border_style="red"))
                    if success_inserts > 0:
                        success_count += 1
                        tables_status[table_name] = True
                        self.console.print(f"[bold green]✓[/bold green] 成功导入 {success_inserts} 条记录到表 [bold cyan]{table_name}[/bold cyan]")
                    else:
                        tables_status[table_name] = False
                        self.console.print(f"[bold red]✗[/bold red] 表 {table_name} 数据导入失败")
                conn.commit()
                status_table = Table(title="数据导入状态", box=box.ROUNDED)
                status_table.add_column("表名", style="cyan")
                status_table.add_column("状态", style="bold")
                status_table.add_column("结果", style="bold")
                for table_name, status in tables_status.items():
                    icon, color = ("✓", "green") if status else ("✗", "red")
                    status_table.add_row(table_name, f"[{color}]{icon}[/{color}]", "成功" if status else "失败")
                self.console.print(status_table)
                if success_count == total_tables:
                    self.console.print(Panel("[bold green]✓[/bold green] 所有数据导入完成！", border_style="green"))
                    return True
                elif success_count > 0:
                    self.console.print(Panel(f"[bold yellow]![/bold yellow] 部分数据导入完成 ({success_count}/{total_tables} 个表)", border_style="yellow"))
                    return False
                else:
                    self.console.print(Panel("[bold red]✗[/bold red] 数据导入完全失败！", border_style="red"))
                    return False
            except sqlite3.Error as e:
                self.console.print(Panel(f"数据库操作失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
                return False
            finally:
                if conn:
                    conn.close()
        except json.JSONDecodeError:
            self.console.print(Panel(f"JSON文件 {json_file} 格式错误", title="[bold red]错误[/bold red]", border_style="red"))
            return False
        except Exception as e:
            self.console.print(Panel(f"导入数据失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            return False