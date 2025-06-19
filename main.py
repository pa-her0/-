import argparse
import os
import sys
import re
import sqlite3
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table, box
from rich.syntax import Syntax
from rich.prompt import Prompt
from prompt_toolkit import PromptSession
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.styles import Style

# 导入自定义模块
from db_init import initialize_database
from schema_manager import SchemaManager
from sql_validator import SQLValidator
from query_visualizer import QueryVisualizer
from nl_query import NLQueryProcessor
from data_exporter import DataExporter
import sqlparse

# --- 自动补全功能 ---
class SQLCompleter(Completer):
    def __init__(self, db_file):
        self.db_file = db_file
        self.keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
            'CREATE', 'TABLE', 'DROP', 'ALTER', 'ADD', 'PRIMARY', 'KEY', 'FOREIGN',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'ON', 'GROUP', 'BY', 'ORDER', 'ASC', 'DESC',
            'LIMIT', 'AND', 'OR', 'NOT', 'NULL', 'IS'
        ]
        self.commands = [
            'show schema', 'show table', 'preview', 'export table', 'export query',
            'help', 'exit', 'init', 'reset', 'data', 'schema', 'sql', 'nl'
        ]
        self.tables = []
        self.columns = set()
        self.update_schema_cache()

    def update_schema_cache(self):
        try:
            if not os.path.exists(self.db_file):
                self.tables, self.columns = [], set()
                return
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            self.tables = [row[0] for row in cursor.fetchall()]
            self.columns = set()
            for table in self.tables:
                cursor.execute(f'PRAGMA table_info("{table}")')
                for col_info in cursor.fetchall():
                    self.columns.add(col_info[1])
            conn.close()
        except sqlite3.Error:
            self.tables, self.columns = [], set()

    def get_completions(self, document, complete_event):
        text_before_cursor = document.text_before_cursor
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        if not text_before_cursor.strip():
            for cmd in self.commands:
                yield Completion(cmd, start_position=0)
            return
        suggestions = self.keywords + self.tables + list(self.columns) + self.commands
        for item in suggestions:
            if item.lower().startswith(word_before_cursor.lower()):
                yield Completion(item, start_position=-len(word_before_cursor))

# --- 控制台设置 ---
console = Console(force_terminal=True)
completer_style = Style.from_dict({
    'completion-menu.completion': 'bg:#008888 #ffffff',
    'completion-menu.completion.current': 'bg:#00aaaa #000000',
    'scrollbar.background': 'bg:#88aaaa',
    'scrollbar.button': 'bg:#222222',
})

def show_welcome_banner():
    title = Text("数据库命令行工具 v1.0", style="bold blue")
    subtitle = Text("用于数据库课程作业的交互式工具", style="italic cyan")
    panel = Panel(
        Text.assemble(title, "\n", subtitle, "\n\n",
                     "✨ 支持SQL查询验证和可视化\n",
                     "✨ 支持自然语言转SQL\n",
                     "✨ 支持关系模式导入和管理\n",
                     "✨ ", Text("命令历史记录", style="bold"), " (↑/↓键查看)\n",
                     "✨ ", Text("自动补全", style="bold"), " (Tab键触发)", justify="center"),
        border_style="green",
        title="[bold yellow]Project 2025[/bold yellow]",
        subtitle="[bold red]数据库系统原理[/bold red]"
    )
    console.print(panel, justify="center")
    
def execute_sql_statements(sql_query, validator, visualizer):
    statements = sqlparse.split(sql_query)
    for statement in statements:
        statement = statement.strip()
        if not statement: continue
        try:
            is_valid, error_message, suggestions = validator.validate(statement)
            if is_valid:
                visualizer.execute_and_visualize(statement)
            else:
                error_panel = Panel(f"{error_message}", title="[bold red]SQL错误[/bold red]", border_style="red")
                console.print(error_panel)
        except Exception as e:
            console.print(Panel(f"处理查询时发生错误: {e}", title="[bold red]错误[/bold red]", border_style="red"))

def handle_export_command(command, exporter):
    table_match = re.match(r'export\s+table\s+([\w_]+)\s+to\s+(.+)', command, re.IGNORECASE)
    if table_match:
        table_name, filename = table_match.groups()
        sql_to_export = f'SELECT * FROM "{table_name}"'
        exporter.export_query_to_file(sql_to_export, filename.strip())
        return

    query_match = re.match(r'export\s+query\s+"([^"]+)"\s+to\s+(.+)', command, re.IGNORECASE)
    if query_match:
        sql_to_export, filename = query_match.groups()
        exporter.export_query_to_file(sql_to_export, filename.strip())
        return
        
    console.print(Panel("导出命令格式不正确。\n正确格式:\n  - export table <表名> to <文件名.csv|json>\n  - export query \"<SQL查询>\" to <文件名.csv|json>", title="[bold red]命令错误[/bold red]", border_style="red"))

def show_help():
    help_table = Table(title="可用命令", box=box.ROUNDED)
    help_table.add_column("命令", style="cyan", width=35)
    help_table.add_column("说明", style="green")
    
    help_table.add_row("help", "显示此帮助信息")
    help_table.add_row("exit", "退出程序")
    help_table.add_row("init / reset", "初始化或重置数据库")
    help_table.add_row("schema / data <文件路径>", "从JSON文件导入模式或数据")
    help_table.add_row("show schema", "显示所有表的概览")
    help_table.add_row("show table <表名>", "显示指定表的详细结构")
    help_table.add_row("preview <表名> [行数]", "预览表的前N行数据（默认5行）")
    help_table.add_row("export table <表名> to <文件名>", "将整个表导出为.csv或.json文件")
    help_table.add_row("export query \"<SQL>\" to <文件名>", "将查询结果导出为.csv或.json文件")
    help_table.add_row("sql <SQL查询> / 直接输入", "执行SQL查询")
    help_table.add_row("nl <自然语言查询>", "执行自然语言查询")
    
    console.print(help_table, justify="center")

def interactive_mode():
    console.print(
        Panel.fit(
            Text.assemble(
                "输入 'help' 查看可用命令, 'exit' 退出。\n",
                "使用 ", Text("↑", style="bold cyan"), " 和 ", Text("↓", style="bold cyan"), " 键可翻阅历史命令。"
            ),
            title="[bold green]交互式模式[/bold green]", border_style="green"
        ), justify="center"
    )
    
    db_file = "project2025.db"
    validator = SQLValidator()
    visualizer = QueryVisualizer()
    schema_manager = SchemaManager()
    exporter = DataExporter()
    nl_processor = NLQueryProcessor()
    completer = SQLCompleter(db_file)

    session = PromptSession(
        history=InMemoryHistory(), 
        completer=completer, 
        style=completer_style
    )

    while True:
        try:
            completer.update_schema_cache()
            command = session.prompt("DB>: ")

            if not command: continue

            cmd_lower = command.lower()
            
            if cmd_lower == 'exit': break
            elif cmd_lower == 'help': show_help()
            elif cmd_lower == 'init':
                if initialize_database():
                    console.print("[bold green]✓[/bold green] 数据库初始化完成！")
            elif cmd_lower == 'reset':
                schema_manager.reset_schema()
            elif cmd_lower.startswith("schema "):
                file_path = command[7:].strip()
                schema_manager.import_schema_from_json(file_path)
            elif cmd_lower.startswith("data "):
                file_path = command[5:].strip()
                schema_manager.import_data_from_json(file_path)
            elif cmd_lower == 'show schema':
                schema_manager.show_schema()
            elif cmd_lower.startswith('show table '):
                table_name = command[11:].strip()
                if table_name: schema_manager.show_table_details(table_name)
                else: console.print("[yellow]请输入要查看的表名: show table <表名>[/yellow]")
            elif cmd_lower.startswith('preview '):
                parts = command.split()
                if len(parts) >= 2:
                    table_name = parts[1]
                    limit = 5
                    if len(parts) > 2 and parts[2].isdigit(): limit = int(parts[2])
                    preview_sql = f'SELECT * FROM "{table_name}" LIMIT {limit}'
                    visualizer.execute_and_visualize(preview_sql)
                else: console.print("[yellow]请输入要预览的表名: preview <表名> [行数][/yellow]")
            elif cmd_lower.startswith('export '):
                handle_export_command(command, exporter)
            elif cmd_lower.startswith("sql ") or any(cmd_lower.startswith(keyword.lower()) for keyword in ["select", "insert", "update", "delete", "create", "drop", "alter"]):
                sql_query = command[4:].strip() if cmd_lower.startswith("sql ") else command
                execute_sql_statements(sql_query, validator, visualizer)
            
            # 【修正点】将自然语言处理的完整逻辑恢复到这里
            elif cmd_lower.startswith("nl "):
                nl_query = command[3:].strip()
                if not nl_query:
                    console.print(Panel("请输入要查询的自然语言内容。", title="[bold yellow]提示[/bold yellow]", border_style="yellow"))
                    continue
                
                try:
                    console.print("[bold magenta]处理自然语言查询...[/bold magenta]")
                    result = nl_processor.process_natural_language_query(nl_query)
                    
                    nl_table = Table(title="自然语言查询结果", show_header=False, box=None)
                    nl_table.add_column("字段", style="bold cyan", width=15)
                    nl_table.add_column("值", style="yellow")
                    nl_table.add_row("自然语言查询", result.get("natural_language_query", nl_query))
                    
                    sql_syntax_display = Syntax(result.get("interpreted_sql", "-- AI未能生成SQL --"), "sql", theme="monokai", line_numbers=True)
                    nl_table.add_row("解释为SQL", "")
                    
                    confidence = result.get("confidence", 0.0)
                    confidence_bar = "["
                    filled = int(confidence * 10)
                    confidence_bar += "█" * filled + "░" * (10 - filled)
                    confidence_bar += f"] {confidence:.2f}"
                    nl_table.add_row("置信度", confidence_bar)
                    
                    console.print(Panel(nl_table, border_style="magenta"))
                    console.print(Panel(sql_syntax_display, title="[bold cyan]生成的SQL[/bold cyan]", border_style="cyan"))

                    if "error" not in result and result.get("interpreted_sql"):
                        execute = Prompt.ask("是否执行生成的SQL查询？", choices=["y", "n"], default="y")
                        if execute.lower() == "y":
                            console.print("[bold]执行查询中...[/bold]")
                            visualizer.execute_and_visualize(result["interpreted_sql"])
                except Exception as e:
                    console.print(Panel(f"处理自然语言查询时发生错误: {e}", title="[bold red]错误[/bold red]", border_style="red"))

            else:
                console.print(Panel("未知命令。输入 'help' 查看可用命令，或使用 'Tab' 键获取提示。", title="[bold yellow]提示[/bold yellow]", border_style="yellow"), justify="center")
        except (KeyboardInterrupt, EOFError):
            break
        except Exception as e:
            console.print(f"[red]发生意外错误: {str(e)}[/red]")

def main():
    show_welcome_banner()
    interactive_mode()

if __name__ == "__main__":
    main()