import matplotlib
import matplotlib.pyplot as plt
import networkx as nx 
import sqlite3
import sqlparse 
# import re 
# import json
import time 
from rich.console import Console 
from rich.table import Table 
from rich.panel import Panel 
from rich.syntax import Syntax 
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn 
from rich.tree import Tree 
from rich import box 

# --- BEGIN Font Configuration for Matplotlib ---
try:
    # 设置一个包含常用中文字体的列表。Matplotlib 会按顺序尝试使用它们。
    # 用户系统上必须安装以下至少一种字体，中文才能正确渲染。
    matplotlib.rcParams['font.sans-serif'] = [
        'SimHei',           # 黑体 (Windows 上常见)
        'Microsoft YaHei',  # 微软雅黑 (Windows 上常见)
        'WenQuanYi Zen Hei',# 文泉驿正黑 (Linux 上常见)
        'Hiragino Sans GB', # 冬青黑体简体中文 (macOS 上常见)
        'Noto Sans CJK SC', # 谷歌思源黑体 (简体中文)
        'DejaVu Sans',      # Matplotlib 默认后备字体
    ]
    # 解决中文环境下负号显示为方块的问题
    matplotlib.rcParams['axes.unicode_minus'] = False  
except Exception as e:
    # 如果在模块导入时设置字体出错，打印警告。
    # Rich Console 可能在此处尚不可用，因此使用标准print。
    print(f"[警告] 初始化 Matplotlib 中文字体设置失败: {e}")
    print(f"[提示] 图表中的中文字符可能无法正确显示。请确保您的系统中已安装至少一种兼容的中文字体 (例如 'SimHei', 'Microsoft YaHei' 等)，并且 Matplotlib 能够找到它。")
# --- END Font Configuration ---

class QueryVisualizer:
    def __init__(self, db_name="project2025"):
        self.db_name = db_name
        self.db_file = f"{db_name}.db"
        self.conn = None
        self.cursor = None
        self.console = Console()

    def _connect_db(self):
        """连接到数据库"""
        if self.conn:  # 如果已经连接，先关闭
            try:
                self.conn.close()
            except sqlite3.Error:
                pass
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.cursor = self.conn.cursor()
            return True
        except sqlite3.Error as e:
            self.console.print(Panel(f"QueryVisualizer: 连接数据库失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            self.conn = None
            self.cursor = None
            return False

    def execute_and_visualize(self, sql_query):
        """执行并可视化SQL查询"""
        if not self._connect_db():
            return False, None, None
            
        try:
            # 显示格式化的SQL
            formatted_sql = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
            sql_syntax = Syntax(formatted_sql, "sql", theme="monokai", line_numbers=True)
            self.console.print(Panel(sql_syntax, title="[bold blue]SQL查询[/bold blue]", border_style="blue", padding=(0, 2)))
            
            # 执行查询
            self.cursor.execute(sql_query)
            
            # 对于非SELECT语句，显示执行成功
            if not sql_query.strip().upper().startswith('SELECT'):
                self.conn.commit()  # 确保提交更改
                self.console.print("[bold green]✓[/bold green] 执行成功")
                self.close()
                return True, None, None
            
            # 对于SELECT语句，获取并显示结果
            results = self.cursor.fetchall()
            
            # 获取列名
            column_names = [description[0] for description in self.cursor.description] if self.cursor.description else []
            
            # 显示查询结果
            if results:
                self._visualize_results_as_table(results, column_names)
            else:
                self.console.print("[yellow]查询执行成功，但没有返回任何数据。[/yellow]")
            
            # 完成后关闭连接
            self.close()
            return True, results, column_names
        except sqlite3.Error as e:
            error_panel = Panel(f"{str(e)}", title="[bold red]查询执行错误[/bold red]", border_style="red")
            self.console.print(error_panel)
            self.close()
            return False, None, None
        except Exception as e_general:
            self.console.print(Panel(f"执行和可视化查询时发生未知错误: {str(e_general)}", title="[bold red]严重错误[/bold red]", border_style="red"))
            self.close()
            return False, None, None

    def _generate_query_plan(self, sql_query): #
        """生成查询计划"""
        if not self.conn or not self.cursor: #
             # Try to reconnect or return an empty plan
            if not self._connect_db():
                self.console.print(Panel("无法连接数据库以生成查询计划。", title="[bold red]错误[/bold red]", border_style="red"))
                return self._parse_sql_manually(sql_query) # Fallback

        # sql_query should already be a single statement from execute_and_visualize
        try:
            self.cursor.execute(f"EXPLAIN QUERY PLAN {sql_query}") #
            sqlite_plan = self.cursor.fetchall() #
            plan = [] #
            for step in sqlite_plan: #
                plan.append({"id": step[0], "parent": step[1], "operation": step[3]}) #
            return plan #
        except sqlite3.Error:
            return self._parse_sql_manually(sql_query) #
    
    def _parse_sql_manually(self, sql_query): #
        """手动解析SQL查询以生成简化的查询计划"""
        parsed = sqlparse.parse(sql_query)[0] #
        plan = [] #
        select_seen = False #
        from_seen = False #
        # ... (other flags and logic) ...
        tables = [] #
        for token in parsed.tokens: #
            if token.ttype is sqlparse.tokens.Keyword: #
                keyword = token.value.upper() #
                if keyword == 'SELECT': #
                    select_seen = True #
                    plan.append({"id": len(plan), "parent": -1, "operation": "SELECT"}) #
                elif keyword == 'FROM': #
                    from_seen = True #
                # ... (other keywords) ...
            if from_seen and isinstance(token, sqlparse.sql.Identifier): #
                tables.append(token.value) #
                plan.append({"id": len(plan), "parent": 0, "operation": f"SCAN TABLE {token.value}"}) #
        if not plan: #
            plan = [{"id": 0, "parent": -1, "operation": "QUERY ROOT"}, {"id": 1, "parent": 0, "operation": "EXECUTE SQL"}] #
        return plan #
    
    def _visualize_results_as_table(self, results, column_names):
        """以表格形式可视化查询结果"""
        if not results:
            self.console.print("[yellow]查询没有返回任何结果[/yellow]")
            return
        
        # 创建表格
        table = Table(box=box.ROUNDED, header_style="bold magenta", show_lines=True)
        for col in column_names:
            table.add_column(col, style="cyan")
        
        # 添加数据行
        for row in results:
            table.add_row(*[str(cell) for cell in row])
        
        # 显示结果
        self.console.print(f"\n[bold green]✓[/bold green] 查询返回 [bold cyan]{len(results)}[/bold cyan] 条结果")
        self.console.print(table)

    def _visualize_query_plan(self, query_plan):
        """可视化查询计划"""
        if not query_plan:
            return
        
        try:
            # 生成图形化查询计划
            G = nx.DiGraph()
            for step in query_plan:
                G.add_node(step["id"], label=step["operation"])
                if step["parent"] >= 0:
                    G.add_edge(step["parent"], step["id"])
            
            plt.figure(figsize=(8, 4))
            pos = nx.spring_layout(G, seed=42)
            nx.draw_networkx_nodes(G, pos, node_size=2000, node_color="lightblue", alpha=0.8, edgecolors="blue")
            nx.draw_networkx_edges(G, pos, width=2, arrowsize=20, edge_color="gray", alpha=0.7)
            labels = {node: data.get("label", f"节点{node}") for node, data in G.nodes(data=True)}
            nx.draw_networkx_labels(G, pos, labels, font_size=8, font_weight="bold")
            
            plt.title("查询执行计划", fontsize=12, pad=10)
            plt.axis("off")
            plt.savefig("query_plan.png", dpi=300, bbox_inches="tight")
            plt.close()
            
        except Exception as e:
            self.console.print(f"[yellow]生成查询计划可视化时出错: {e}[/yellow]")

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                self.cursor = None
            except sqlite3.Error as e:
                self.console.print(Panel(f"QueryVisualizer: 关闭数据库连接时出错: {e}", title="[bold red]错误[/bold red]", border_style="red"))

    def __del__(self):
        """确保对象销毁时关闭连接"""
        self.close()
