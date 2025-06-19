import nltk
import re
import sqlite3
import jieba
import requests
import json
import logging
import sqlparse # 引入 sqlparse 库
from nltk.corpus import stopwords
from rich.console import Console
from typing import Dict, List, Optional

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 下载 NLTK 数据 (不变)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

class NLQueryProcessor:
    def __init__(self, db_name: str = "project2025", ai_api_url: str = "https://api.moonshot.cn/v1/chat/completions", ai_model: str = "moonshot-v1-8k"):
        self.db_name = db_name
        self.db_file = f"{db_name}.db"
        self.console = Console(force_terminal=True)
        self.ai_api_url = ai_api_url
        self.ai_api_key = "sk-VYBmpclwdeWRs9lwrK2SnRlISJ3gTMErssLmopDRxvlhg7yT" # 请替换为您的有效API密钥
        self.ai_model = ai_model
        
        self.conn = None
        self.cursor = None
        self.tables = []
        self.columns = {}
        self.foreign_keys = {}
        self.stop_words = set(stopwords.words('english')).union({"的", "了", "是", "在", "和", "与"})
        
        self.table_aliases = {
            "Student": ["学生", "同学", "学员"],
            "Teacher": ["教师", "老师", "讲师", "教授"],
            "Course": ["课程", "科目", "学科", "课"],
            "SC": ["选课", "成绩"],
            "TC": ["授课", "教授课程"]
        }
        self.column_aliases = {
            "Student": {"Sno": ["学号"], "Sname": ["姓名"], "Ssex": ["性别"], "Sage": ["年龄"], "Sdept": ["系"], "Scholarship": ["奖学金"]},
            "Teacher": {"Tno": ["工号"], "Tname": ["教师姓名"], "Tsex": ["性别"], "Tage": ["年龄"], "Tdept": ["系"], "Trank": ["职称"]},
            "Course": {"Cno": ["课程号"], "Cname": ["课程名"], "Cpno": ["先修课"], "Ccredit": ["学分"]}
        }
        
        self.table_mappings = {}
        self.column_mappings = {}

    def _connect_db(self):
        if self.conn: return True
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.cursor = self.conn.cursor()
            return True
        except sqlite3.Error as e:
            logging.error(f"数据库连接失败: {str(e)}")
            self.conn = None
            self.cursor = None
            return False

    def _update_mappings_and_schema(self):
        if not self._connect_db(): return False
        self.tables = self._get_tables()
        self.columns = self._get_columns()
        self.foreign_keys = self._get_foreign_keys()
        self.table_mappings = {alias: table for table, aliases in self.table_aliases.items() for alias in aliases}
        self.table_mappings.update({table.lower(): table for table in self.tables})
        self.column_mappings = {
            alias: (table, col) 
            for table, col_map in self.column_aliases.items() 
            for col, aliases in col_map.items() 
            for alias in aliases
        }
        return True

    def _get_tables(self):
        if not self.cursor: return []
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [table[0] for table in self.cursor.fetchall() if table[0] != 'sqlite_sequence']

    def _get_columns(self):
        if not self.cursor: return {}
        columns_map = {}
        for table in self.tables:
            self.cursor.execute(f'PRAGMA table_info("{table}");')
            columns_map[table] = [col[1] for col in self.cursor.fetchall()]
        return columns_map

    def _get_foreign_keys(self):
        foreign_keys = {}
        for table in self.tables:
            self.cursor.execute(f'PRAGMA foreign_key_list("{table}");')
            foreign_keys[table] = [(row[3], row[2], row[4]) for row in self.cursor.fetchall()]
        return foreign_keys

    def process_natural_language_query(self, nl_query: str) -> Dict:
        if not self._update_mappings_and_schema():
            return {"natural_language_query": nl_query, "interpreted_sql": "-- 无法连接数据库以加载模式", "confidence": 0.0, "error": "数据库连接失败"}

        logging.info(f"处理查询: {nl_query}")
        
        schema_info = {"tables": self.columns, "foreign_keys": self.foreign_keys, "aliases": {"tables": self.table_aliases, "columns": self.column_aliases}}
        
        sql_query = self._call_ai_model(nl_query, schema_info)
        
        if not sql_query or sql_query.strip().startswith("--"):
            return {"natural_language_query": nl_query, "interpreted_sql": sql_query or "-- AI未能生成SQL --", "confidence": 0.1, "error": "AI无法解析查询"}
        
        validated_sql = self._validate_sql(sql_query)
        confidence = self._calculate_confidence(validated_sql, nl_query)
        
        return {"natural_language_query": nl_query, "interpreted_sql": validated_sql, "confidence": confidence}

    def _call_ai_model(self, nl_query: str, schema_info: Dict) -> Optional[str]:
        prompt = f"""
你是一个数据库专家，需要将以下自然语言查询转换为SQL。数据库模式如下：
{json.dumps(schema_info, ensure_ascii=False, indent=2)}

自然语言查询：{nl_query}

请生成正确的SQL查询，仅返回SQL语句，不要包含其他说明。确保：
1. 使用双引号括住表名和列名（如 "Student"."Sname"）。
2. 正确处理表之间的外键关系。
3. 支持SELECT、WHERE、JOIN、ORDER BY、LIMIT等。
4. 如果查询无法解析，返回"-- 无法解析查询"。
"""
        try:
            headers = {"Authorization": f"Bearer {self.ai_api_key}", "Content-Type": "application/json"}
            payload = {"model": self.ai_model, "messages": [{"role": "system", "content": "You are a database expert who converts natural language to SQL."}, {"role": "user", "content": prompt}], "max_tokens": 200, "temperature": 0.3}
            response = requests.post(self.ai_api_url, headers=headers, json=payload, timeout=20)
            response.raise_for_status()
            result = response.json()
            return result.get("choices", [{}])[0].get("message", {}).get("content", "-- 无法解析查询").strip()
        except requests.RequestException as e:
            logging.error(f"AI API调用失败: {str(e)}")
            return "-- AI API调用失败"
        except json.JSONDecodeError:
            logging.error("AI API响应解析失败")
            return "-- AI响应无效"

    def _validate_sql(self, sql_query: str) -> str:
        """【修正点】使用 sqlparse 更智能地验证SQL"""
        if sql_query.strip().startswith("--"): return sql_query
        if not self._connect_db(): return "-- 数据库连接失败"
        
        # 使用 sqlparse 提取查询中的真实表名
        parsed = sqlparse.parse(sql_query)[0]
        tables_in_query = set()
        
        # 递归函数来提取表名
        def extract_tables_from_tokens(tokens):
            from_or_join_seen = False
            for token in tokens:
                if token.is_group:
                    extract_tables_from_tokens(token.tokens)
                if token.is_keyword and token.value.upper() in ('FROM', 'JOIN'):
                    from_or_join_seen = True
                elif from_or_join_seen and isinstance(token, sqlparse.sql.Identifier):
                    tables_in_query.add(token.get_real_name())
                elif from_or_join_seen and isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        tables_in_query.add(identifier.get_real_name())
                elif token.ttype is sqlparse.tokens.Keyword and from_or_join_seen:
                    # 如果遇到另一个关键字（不是AS或ON），则FROM/JOIN子句结束
                    if token.value.upper() not in ['AS', 'ON', ',']:
                        from_or_join_seen = False

        extract_tables_from_tokens(parsed.tokens)

        # 只验证提取出的表名
        for table_name in tables_in_query:
            if table_name not in self.tables:
                logging.warning(f"无效表名: {table_name}")
                return f"-- 无效表名: {table_name}"
        
        # 使用 EXPLAIN 验证语法的其余部分
        try:
            self.cursor.execute(f"EXPLAIN QUERY PLAN {sql_query}")
            return sql_query
        except sqlite3.Error as e:
            logging.error(f"SQL语法错误: {str(e)}")
            return f"-- SQL语法错误: {str(e)}"

    def _calculate_confidence(self, sql_query: str, nl_query: str) -> float:
        if not sql_query or sql_query.startswith("--"): return 0.1
        confidence = 0.5
        if "WHERE" in sql_query.upper(): confidence += 0.2
        if "JOIN" in sql_query.upper(): confidence += 0.2
        if len(re.findall(r'"\w+"', sql_query)) > 2: confidence += 0.1
        return min(confidence, 1.0)

    def close(self):
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                self.cursor = None
            except sqlite3.Error:
                logging.error(f"关闭数据库连接时出错")

    def __del__(self):
        self.close()