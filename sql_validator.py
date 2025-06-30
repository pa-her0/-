import sqlite3
import sqlparse
import re
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich import box
from rich.text import Text

class SQLValidator:
    def __init__(self, db_name="project2025"):
        self.db_name = db_name
        self.db_file = f"{db_name}.db"
        self.console = Console()
        self.conn = None
        self.cursor = None
        self.tables = {}
        self.columns = {}
    
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
            self.console.print(Panel(f"连接数据库失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            self.conn = None
            self.cursor = None
            return False
    
    def _load_schema(self):
        """加载数据库模式信息"""
        if not self._connect_db():
            self.console.print(Panel("数据库未连接，无法加载模式。", title="[bold red]错误[/bold red]", border_style="red"))
            return False
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = self.cursor.fetchall()
            
            self.tables.clear()  # 清空旧的模式信息
            self.columns.clear()

            for table_tuple in tables:
                table_name = table_tuple[0]
                self.tables[table_name] = []
                
                self.cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
                columns_info = self.cursor.fetchall()
                
                for col_info in columns_info:
                    column_name = col_info[1]
                    self.tables[table_name].append(column_name)
                    
                    if column_name not in self.columns:
                        self.columns[column_name] = []
                    self.columns[column_name].append(table_name)
            
            # 完成后关闭连接
            self.close()
            return True
        except sqlite3.Error as e:
            self.console.print(Panel(f"加载数据库模式失败: {str(e)}", title="[bold red]错误[/bold red]", border_style="red"))
            self.close()
            return False
    
    def validate(self, sql_query):
        """验证SQL查询"""
        formatted_sql = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
        sql_syntax = Syntax(formatted_sql, "sql", theme="monokai", line_numbers=True)
        self.console.print(Panel(sql_syntax, title="[bold blue]验证SQL查询[/bold blue]", border_style="blue"))
        
        if not self._connect_db():
            return False, "数据库连接失败，无法验证。", []
            
        self._load_schema()  # 重新加载模式信息
        
        # 重新连接数据库，因为_load_schema会关闭连接
        if not self._connect_db():
            return False, "数据库连接失败，无法验证。", []

        if not sql_query.strip():
            self.close()
            return False, "SQL查询不能为空。", ["请输入有效的SQL查询。"]

        if not self._check_basic_syntax(sql_query):
            self.close()
            return False, "SQL语法错误", ["检查SQL语法是否正确", "确保关键字拼写正确", "检查括号是否匹配"]
        
        parsed_statements = sqlparse.parse(sql_query)
        if not parsed_statements:
            self.close()
            return False, "无法解析SQL查询。", ["请检查查询格式。"]
        if len(parsed_statements) > 1:
            self.close()
            return False, "只允许验证单个SQL语句。", ["请将查询拆分为多个独立的语句进行验证。"]
        
        parsed = parsed_statements[0]
        single_sql_query_for_explain = str(parsed)

        tables_in_query = self._extract_tables(parsed)
        invalid_tables = [table for table in tables_in_query if table not in self.tables]
        
        try:
            self.cursor.execute(f"EXPLAIN {single_sql_query_for_explain}")
            self.console.print(Panel("[bold green]✓[/bold green] SQL查询验证通过！", border_style="green"))
            self.close()
            return True, None, None
        except sqlite3.Error as e:
            error_msg = str(e)
            suggestions = self._generate_suggestions_for_error(error_msg, tables_in_query, [])
            self.console.print(Panel(f"[bold red]SQL执行错误 (EXPLAIN):[/bold red] {error_msg}", border_style="red"))
            self.close()
            return False, error_msg, suggestions
    
    def _check_basic_syntax(self, sql_query):
        """检查基本SQL语法"""
        try:
            if sql_query.count('(') != sql_query.count(')'): 
                return False
            parsed = sqlparse.parse(sql_query)[0] 
            if not parsed.tokens: # 空语句或解析失败
                 return False
            keywords = [token.value.upper() for token in parsed.tokens if token.ttype is sqlparse.tokens.Keyword] 
            if 'SELECT' in keywords and 'FROM' not in keywords: 
                is_dml_select = any(token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT' for token in parsed.tokens)
                if is_dml_select and 'FROM' not in keywords :
                     pass # Relaxing this for now as `EXPLAIN` will catch it.
            return True
        except Exception:
            return False
    
    def _extract_tables(self, parsed):
        """从解析的SQL中提取表名"""
       
        tables = []
        from_seen = False
        # Iterate through tokens to find table names after FROM or JOIN
        for token in parsed.flatten(): # flatten() helps to iterate through all tokens recursively
            if token.is_keyword and token.value.upper() in ('FROM', 'JOIN'):
                from_seen = True
                continue
            if from_seen and isinstance(token, sqlparse.sql.Identifier):
                tables.append(token.get_real_name()) #
                from_seen = False # Reset after finding one, or handle IdentifierList
            elif from_seen and isinstance(token, sqlparse.sql.IdentifierList): #
                for identifier in token.get_identifiers(): #
                    tables.append(identifier.get_real_name()) #
                from_seen = False
            # Stop searching for tables once keywords like WHERE, GROUP BY, ORDER BY, etc. are encountered
            # or if another DML keyword starts a new clause, unless it's a subquery context.
            if token.is_keyword and token.value.upper() not in ('FROM', 'JOIN', 'ON', 'AS') and from_seen:
                 from_seen = False # End of FROM/JOIN clause for simple cases
        
        tables_found = []
        from_or_join_seen = False
        for token in parsed.tokens:
            if token.is_keyword and token.value.upper() in ['FROM', 'JOIN']:
                from_or_join_seen = True
            elif from_or_join_seen:
                if isinstance(token, sqlparse.sql.Identifier):
                    tables_found.append(token.get_real_name())
                    # If it's a JOIN, the next identifier might be another table after ON or USING
                    # For simplicity, after FROM, we usually expect table names.
                    # If after JOIN, we expect a table name.
                    # This simplistic approach doesn't handle complex JOINs or subqueries well.
                elif isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        tables_found.append(identifier.get_real_name())
                
                # Heuristic: if we see another keyword that usually follows a list of tables, stop.
                if token.is_keyword and token.value.upper() not in ['AS', 'ON', ',']: # 'AS' for alias, 'ON' for join condition
                    from_or_join_seen = False # Reset
            if isinstance(token, sqlparse.sql.Statement): # Recurse for subqueries if any
                tables_found.extend(self._extract_tables(token))
        
        return list(set(tables_found)) # Return unique table names

    def _extract_columns(self, parsed): #
        """从解析的SQL中提取列名 (简化版)"""
        # (代码与之前提供的基本一致, 使用 sqlparse 遍历 token)
        # This is a simplified extractor. Does not handle aliases for columns well for validation purposes
        # or all complex expressions.
        columns_found = []
        
        # Check if it's a SELECT statement
        if parsed.get_type() != 'SELECT':
            return columns_found

        select_seen = False
        from_seen = False # To stop looking for columns once FROM is reached

        for token in parsed.tokens:
            if from_seen: # Stop if we have reached the FROM clause
                break
            
            if token.is_keyword and token.value.upper() == 'SELECT':
                select_seen = True
                continue
            
            if token.is_keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue

            if select_seen:
                if isinstance(token, sqlparse.sql.Identifier):
                    # Check if it's a function call like COUNT(*)
                    if token.tokens[-1].match(sqlparse.tokens.Punctuation, '()'): 
                        if '.' in token.value: 
                            parts = token.value.split('.', 1) 
                            table_part = parts[0].strip('"') 
                            column_part = parts[1].strip('"') 
                            columns_found.append((column_part, table_part)) 
                        else:
                            columns_found.append((token.get_real_name(), None)) 
                    else: # Not a function call, simple identifier
                        if '.' in token.value: 
                            parts = token.value.split('.', 1) 
                            table_part = parts[0].strip('"') 
                            column_part = parts[1].strip('"') 
                            columns_found.append((column_part, table_part)) 
                        else:
                            columns_found.append((token.get_real_name(), None)) 

                elif isinstance(token, sqlparse.sql.IdentifierList): 
                    for identifier_item in token.get_identifiers(): 
                        if isinstance(identifier_item, sqlparse.sql.Identifier): 
                            if '.' in identifier_item.value: 
                                parts = identifier_item.value.split('.', 1) 
                                table_part = parts[0].strip('"') 
                                column_part = parts[1].strip('"') 
                                columns_found.append((column_part, table_part)) 
                            else:
                                columns_found.append((identifier_item.get_real_name(), None)) 
                elif token.ttype is sqlparse.tokens.Wildcard: 
                    columns_found.append(('*', None)) 
        
        return columns_found

    def _find_similar_names(self, name, name_list, threshold=0.6): 
        similar_names = [] 
        for n in name_list: 
            similarity = self._calculate_similarity(name.lower(), n.lower()) 
            if similarity >= threshold: 
                similar_names.append(n) 
        return similar_names 
    
    def _calculate_similarity(self, s1, s2): 
        if len(s1) == 0 or len(s2) == 0: return 0.0 
        distance = self._levenshtein_distance(s1, s2) 
        max_len = max(len(s1), len(s2)) 
        return 1.0 - (distance / max_len) 

    def _levenshtein_distance(self, s1, s2): 
        if len(s1) < len(s2): return self._levenshtein_distance(s2, s1) 
        if len(s2) == 0: return len(s1) 
        previous_row = range(len(s2) + 1) 
        for i, c1 in enumerate(s1): 
            current_row = [i + 1] 
            for j, c2 in enumerate(s2): 
                insertions = previous_row[j + 1] + 1 
                deletions = current_row[j] + 1 
                substitutions = previous_row[j] + (c1 != c2) 
                current_row.append(min(insertions, deletions, substitutions)) 
            previous_row = current_row 
        return previous_row[-1] 

    def _generate_suggestions_for_error(self, error_msg, tables, columns): 
        suggestions = [] 
        if "no such table" in error_msg.lower(): 
            suggestions.append("检查表名是否正确拼写") 
            suggestions.append("确保表已经创建") 
        elif "no such column" in error_msg.lower(): 
            suggestions.append("检查列名是否正确拼写") 
            suggestions.append("确保在正确的表中引用列") 
        elif "syntax error" in error_msg.lower(): 
            suggestions.append("检查SQL语法是否正确") 
            suggestions.append("确保关键字拼写正确") 
            suggestions.append("检查括号、逗号等标点符号是否正确") 
        elif "ambiguous" in error_msg.lower(): 
            suggestions.append("使用表名限定列名，例如 'table.column'") 
        elif "constraint" in error_msg.lower(): 
            suggestions.append("检查是否违反了表的约束条件") 
            suggestions.append("确保插入或更新的数据符合表的约束") 
        if not suggestions: 
            suggestions = ["检查SQL语法", "确保所有引用的表和列都存在", "检查数据类型是否匹配"] 
        return suggestions 

    def get_schema_info(self): 
        """获取数据库模式信息"""
        if not self.tables: # 如果模式尚未加载或为空
            self._load_schema() # 尝试加载

        schema_table = Table(title="数据库模式信息", box=box.ROUNDED, header_style="bold magenta", show_lines=True) 
        schema_table.add_column("表名", style="cyan") 
        schema_table.add_column("列", style="green") 
        
        for table_name_info, columns_list in self.tables.items(): 
            schema_table.add_row(table_name_info, ", ".join(columns_list)) 
        
        self.console.print(Panel(schema_table, title="[bold blue]数据库模式[/bold blue]", border_style="blue")) 
        return self.tables 

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                self.cursor = None
            except sqlite3.Error as e:
                self.console.print(Panel(f"SQLValidator: 关闭数据库连接时出错: {e}", title="[bold red]错误[/bold red]", border_style="red"))

    def __del__(self):
        """确保对象销毁时关闭连接"""
        self.close()
