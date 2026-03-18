#!/usr/bin/env python3
"""
DAIL-SQL FastAPI Server
======================

Provides complete DAIL-SQL Text-to-SQL API service, including:
1. Data preprocessing & Schema linking 
2. Few-shot example selection & Prompt construction
3. LLM invocation & SQL validation & Auto retry

API Endpoints:
- POST /api/v1/text-to-sql - Complete DAIL-SQL pipeline
- GET /api/v1/databases - Get available database list  
- GET /api/v1/health - Health check
"""

import os
import sys
import json
import time
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime

# Manually load .env file
def load_env():
    env_path = '.env'
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

load_env()  # Load .env file

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
import sqlite3
import shutil
import tempfile
from pathlib import Path

# Add project path
sys.path.append('./')

# Import DAIL-SQL core components
from utils.linking_process import SpiderEncoderV2Preproc
from utils.datasets.spider import load_tables, Schema, Table, Column
from prompt.prompt_builder import prompt_factory, get_repr_cls, get_example_format_cls, get_example_selector
from utils.data_builder import load_data
from utils.enums import REPR_TYPE, EXAMPLE_TYPE, SELECTOR_TYPE, LLM
from utils.utils import get_tables  # Add get_tables import
from test_validation.sql_validator_v2 import ImprovedSQLValidator
from llm.chatgpt import ask_llm, init_chatgpt
from utils.post_process import process_duplication
import networkx as nx
import sqlite3
# Use custom SQL extraction function to avoid importing files with syntax errors

# ================================
# Request/Response Models
# ================================

class TextToSQLRequest(BaseModel):
    """Text-to-SQL request model"""
    question: str = Field(..., description="Natural language question")
    database_id: str = Field(..., description="Database ID")
    
    # Configuration parameters (optional, supports None values)
    model: Optional[str] = Field(default="gpt-4", description="LLM model")
    temperature: Optional[float] = Field(default=0.1, description="Generation temperature")
    max_retries: Optional[int] = Field(default=3, description="Maximum retry attempts")
    k_shot: Optional[int] = Field(default=3, description="Number of few-shot examples")
    
    # Advanced configuration (optional, supports None values)
    use_self_consistency: Optional[bool] = Field(default=False, description="Whether to use self-consistency voting")
    n_candidates: Optional[int] = Field(default=1, description="Number of SQL candidates")

class SQLResult(BaseModel):
    """Single SQL result"""
    sql: str
    confidence: float = 0.0
    execution_result: Optional[Any] = None
    validation_passed: bool = False
    errors: List[str] = []

class TextToSQLResponse(BaseModel):
    """Text-to-SQL response model"""
    success: bool
    sql_results: List[SQLResult]
    best_sql: str
    
    # Execution information
    execution_time: float
    attempts: int
    processing_steps: List[str]
    
    # Error information
    error: Optional[str] = None
    
    # Debug information
    database_schema: Optional[str] = None
    selected_examples: Optional[List[Dict]] = None
    prompt_used: Optional[str] = None

class DatabaseInfo(BaseModel):
    """Database information"""
    database_id: str
    tables: List[str]
    description: Optional[str] = None

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    version: str = "1.0.0"

class UploadDatabaseResponse(BaseModel):
    """Upload database response"""
    success: bool
    database_id: str
    message: str
    tables: Optional[List[str]] = None
    error: Optional[str] = None

class GeneratePromptRequest(BaseModel):
    """Request to generate a DAIL-SQL prompt (for Copilot integration)"""
    question: str = Field(..., description="Natural language question")
    database_id: str = Field(..., description="Database ID")
    k_shot: Optional[int] = Field(default=3, description="Number of few-shot examples")

class GeneratePromptResponse(BaseModel):
    """Response containing the constructed prompt and a session ID for follow-up validation"""
    success: bool
    session_id: str
    prompt: str
    database_id: str
    question: str
    error: Optional[str] = None

class ValidateSQLRequest(BaseModel):
    """Request to validate a SQL string returned by Copilot"""
    session_id: str = Field(..., description="Session ID from generate-prompt")
    sql: str = Field(..., description="SQL string to validate")
    attempt: int = Field(default=1, description="Current attempt number (1-based)")
    max_attempts: int = Field(default=3, description="Maximum allowed attempts")

class ValidateSQLResponse(BaseModel):
    """Validation result; includes a refined prompt when validation fails and retries remain"""
    valid: bool
    sql: str
    errors: List[str] = []
    # If invalid and retries remain, a corrective prompt is returned
    next_prompt: Optional[str] = None
    attempts_remaining: int = 0

# ================================
# DAIL-SQL Core Processor
# ================================

class DAILSQLProcessor:
    """DAIL-SQL core processor"""
    
    def __init__(self, 
                 dataset_dir: str = "./dataset",
                 db_dir: str = "./dataset/spider/database",
                 openai_api_key: str = None):
        self.dataset_dir = dataset_dir
        self.db_dir = db_dir
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        
        # User uploaded database directory
        self.custom_db_dir = "./dataset/custom_databases"
        self.custom_tables_file = os.path.join(self.custom_db_dir, "custom_tables.json")
        
        # Ensure custom database directory exists
        os.makedirs(self.custom_db_dir, exist_ok=True)
        
        # Initialize DAIL-SQL components
        self.data_loader = None
        self.preprocessor = None
        self.llm_initialized = False
        
        # DAIL-SQL configuration
        self.repr_type = REPR_TYPE.OPENAI_DEMOSTRATION
        self.example_type = EXAMPLE_TYPE.QA  
        self.selector_type = SELECTOR_TYPE.MASKED_CACHED  # Use masked cached selector for EUCDISQUESTIONMASK performance
        
        self._initialize_components()
        self._initialize_custom_tables()
    
    def _initialize_components(self):
        """Initialize DAIL-SQL components"""
        try:
            print("🔄 Initializing DAIL-SQL components...")
            
            # Initialize LLM client
            if self.openai_api_key:
                init_chatgpt(self.openai_api_key, None, "gpt-4")
                self.llm_initialized = True
            
            # Initialize data loader and preprocessor on demand
            print("✅ DAIL-SQL component initialization complete")
            
        except Exception as e:
            print(f"❌ Component initialization failed: {str(e)}")
            raise e
    
    def _load_schema_info(self, database_id: str) -> Dict:
        """Load database schema information (supports pre-configured and custom databases)"""
        # First check pre-configured databases
        tables_json_path = os.path.join(self.dataset_dir, "spider", "tables.json")
        if os.path.exists(tables_json_path):
            with open(tables_json_path, 'r', encoding='utf-8') as f:
                tables_data = json.load(f)
            
            for table_info in tables_data:
                if table_info['db_id'] == database_id:
                    return table_info
        
        # If not found, check custom databases
        if os.path.exists(self.custom_tables_file):
            with open(self.custom_tables_file, 'r', encoding='utf-8') as f:
                custom_tables_data = json.load(f)
            
            for table_info in custom_tables_data:
                if table_info['db_id'] == database_id:
                    return table_info
        
        return None
    
    def _build_fallback_prompt(self, question: str, database_id: str, k_shot: int, schema_info: Dict) -> Dict:
        """Fallback simple prompt when DAIL-SQL components fail"""
        print("⚠️ Using fallback simple prompt")
        
        # Build simple prompt
        prompt_parts = []
        
        # 1. Database schema
        prompt_parts.append("Given the following database schema:")
        prompt_parts.append("")
        prompt_parts.append(schema_info['schema_text'])
        prompt_parts.append("")
        
        # 3. Current question
        prompt_parts.append("Now, please generate a SQL query for the following question:")
        prompt_parts.append(f"Question: {question}")
        prompt_parts.append("")
        prompt_parts.append("SQL:")
        
        full_prompt = "\n".join(prompt_parts)
        
        return {
            "prompt": full_prompt,
            "examples": [],
            "schema": schema_info['schema_text']
        }
    
    def _initialize_custom_tables(self):
        """Initialize custom database table file"""
        if not os.path.exists(self.custom_tables_file):
            with open(self.custom_tables_file, 'w', encoding='utf-8') as f:
                json.dump([], f, indent=2)
    
    def _extract_schema_from_sqlite(self, db_path: str, database_id: str) -> Dict:
        """从SQLite文件提取schema信息"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
            
            # Build schema information
            table_names = tables
            table_names_original = tables
            column_names = []
            column_names_original = []
            column_types = []
            primary_keys = []
            foreign_keys = []
            
            # Add the common "* column"
            column_names.append([-1, "*"])
            column_names_original.append(["", "*"])
            column_types.append("text")
            
            col_index = 1
            for table_idx, table_name in enumerate(tables):
                #get the column information
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                for col in columns:
                    cid, name, col_type, notnull, default, pk = col
                    
                    # add column information
                    column_names.append([table_idx, name.lower()])
                    column_names_original.append([table_idx, name])
                    
                    # Simplify data type mapping
                    if 'int' in col_type.lower() or 'real' in col_type.lower() or 'num' in col_type.lower():
                        column_types.append("number")
                    else:
                        column_types.append("text")
                    
                    # Handle the primary key
                    if pk:
                        primary_keys.append(col_index)
                    
                    col_index += 1
                
                # Obtain foreign key information
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                fks = cursor.fetchall()
                for fk in fks:
                    # This needs to be constructed based on the actual foreign key information.
                    # For now, skip the foreign key processing. It can be expanded in the future.
                    pass
            
            conn.close()
            
            # Build a complete schema dictionary
            schema_dict = {
                "db_id": database_id,
                "table_names": table_names,
                "table_names_original": table_names_original,
                "column_names": column_names,
                "column_names_original": column_names_original,
                "column_types": column_types,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys
            }
            
            return schema_dict
            
        except Exception as e:
            raise Exception(f"Failed to extract the SQLite schema: {str(e)}")
    
    def _initialize_custom_tables(self):
        """Initialize the custom database table file"""
        if not os.path.exists(self.custom_tables_file):
            with open(self.custom_tables_file, 'w', encoding='utf-8') as f:
                json.dump([], f, indent=2)
    
    def _extract_schema_from_sqlite(self, db_path: str, database_id: str) -> Dict:
        """Extract schema information from the SQLite file"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Retrieve all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
            
            # Build schema information
            table_names = tables
            table_names_original = tables
            column_names = []
            column_names_original = []
            column_types = []
            primary_keys = []
            foreign_keys = []
            
            # Add the common "* column"
            column_names.append([-1, "*"])
            column_names_original.append(["", "*"])
            column_types.append("text")
            
            col_index = 1
            for table_idx, table_name in enumerate(tables):
                # Obtain the column information of the table
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                for col in columns:
                    cid, name, col_type, notnull, default, pk = col
                    
                    # Add column information
                    column_names.append([table_idx, name.lower()])
                    column_names_original.append([table_idx, name])
                    
                    # Simplify data type mapping
                    if 'int' in col_type.lower() or 'real' in col_type.lower() or 'num' in col_type.lower():
                        column_types.append("number")
                    else:
                        column_types.append("text")
                    
                    # Handle the primary key
                    if pk:
                        primary_keys.append(col_index)
                    
                    col_index += 1
                
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                fks = cursor.fetchall()
                for fk in fks:
                    pass
            
            conn.close()
            
            # Build a complete schema dictionary
            schema_dict = {
                "db_id": database_id,
                "table_names": table_names,
                "table_names_original": table_names_original,
                "column_names": column_names,
                "column_names_original": column_names_original,
                "column_types": column_types,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys
            }
            
            return schema_dict
            
        except Exception as e:
            raise Exception(f"提取SQLite schema失败: {str(e)}")
    
    def _get_db_path(self, database_id: str) -> str:
        """Get database file path (supports both preconfigured and custom databases)"""
        # check the pre-configured database
        db_path = os.path.join(self.db_dir, database_id, f"{database_id}.sqlite")
        if os.path.exists(db_path):
            return db_path
            
        # Recheck the custom database
        custom_db_path = os.path.join(self.custom_db_dir, f"{database_id}.sqlite") 
        if os.path.exists(custom_db_path):
            return custom_db_path
            
        return None
    
    async def process_text_to_sql(self, request: TextToSQLRequest) -> TextToSQLResponse:
        """Process Text-to-SQL request"""
        start_time = time.time()
        processing_steps = []
        
        try:
            processing_steps.append("✅ Database verification")
            
            if request.database_id:
                db_path = self._get_db_path(request.database_id)
                if not db_path:
                    raise HTTPException(status_code=400, detail=f"database {request.database_id} is invalid")
                        
                processing_steps.append(f"✅ Database verification successful: {request.database_id}")
            
            # 2. Schema linking & preprocessing
            processing_steps.append("🔄 Schema linking preprocessing")
            schema_info = await self._run_preprocessing(request.question, request.database_id)
            processing_steps.append(f"✅ Preprocessing completed - found {len(schema_info['tables']['table_names_original'])} tables")
            
            # 3. Few-shot示例选择 & Prompt构建
            processing_steps.append("🔄 build Few-shot Prompt")
            k_shot = request.k_shot if request.k_shot is not None else 3
            prompt_info = await self._build_prompt(
                request.question, 
                request.database_id, 
                k_shot,
                schema_info
            )
            processing_steps.append(f"✅ Prompt builded - length: {len(prompt_info['prompt'])} chars")
            
            # 4. LLM invocation & SQL validation & retry
            processing_steps.append("🔄 LLM invocation and SQL validation")
            sql_results = await self._execute_llm_with_validation(
                prompt_info,
                request,
                processing_steps
            )
            
            # 5. choise the bestSQL
            use_self_consistency = request.use_self_consistency if request.use_self_consistency is not None else False
            best_sql = self._select_best_sql(sql_results, use_self_consistency)
            
            execution_time = time.time() - start_time
            
            return TextToSQLResponse(
                success=True,
                sql_results=sql_results,
                best_sql=best_sql,
                execution_time=execution_time,
                attempts=len(sql_results),
                processing_steps=processing_steps,
                database_schema=schema_info.get("schema_text"),
                selected_examples=prompt_info.get("examples"),
                prompt_used=prompt_info.get("prompt")
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            return TextToSQLResponse(
                success=False,
                sql_results=[],
                best_sql="",
                execution_time=execution_time,
                attempts=0,
                processing_steps=processing_steps,
                error=str(e)
            )
    
    async def _run_preprocessing(self, question: str, database_id: str) -> Dict:
        """Run genuine DAIL-SQL preprocessing and Schema linking"""
        try:
            # Initialize the data loader on demand
            if not self.data_loader:
                print("🔄 Initialize the data loader...")
                self.data_loader = load_data(
                    data_type="spider",
                    path_data=self.dataset_dir
                )
            
            # Get database schema information (Dict format)
            schema_dict = self._load_schema_info(database_id)
            if not schema_dict:
                raise Exception(f"did not found the schema information of {database_id}")
            
            # Initialize preprocessor on demand  
            if not self.preprocessor:
                print("🔄 Initializing DAIL-SQL preprocessor...")
                self.preprocessor = SpiderEncoderV2Preproc(
                    save_path=self.dataset_dir,
                    compute_sc_link=True,
                    compute_cv_link=True
                )
            
            # DAIl-SQL preprocess
            print(f"🔄 运行Schema linking for {database_id}...")
            try:
                item = {
                    "question": question,
                    "question_toks": question.split(),
                    "db_id": database_id
                }
                
                import sqlite3
                
                tables = tuple(
                    Table(
                        id=i,
                        name=name.split(),
                        unsplit_name=name,
                        orig_name=orig_name,
                    )
                    for i, (name, orig_name) in enumerate(zip(
                        schema_dict['table_names'], schema_dict['table_names_original']))
                )
                
                columns = tuple(
                    Column(
                        id=i,
                        table=tables[table_id] if table_id >= 0 else None,
                        name=col_name.split(),
                        unsplit_name=col_name,
                        orig_name=orig_col_name,
                        type=col_type,
                    )
                    for i, ((table_id, col_name), (_, orig_col_name), col_type) in enumerate(zip(
                        schema_dict['column_names'], 
                        schema_dict['column_names_original'],
                        schema_dict['column_types']))
                )
                
                foreign_key_graph = nx.DiGraph()
                for source_column_id, dest_column_id in schema_dict.get('foreign_keys', []):
                    if source_column_id < len(columns) and dest_column_id < len(columns):
                        source_column = columns[source_column_id]
                        dest_column = columns[dest_column_id]
                        foreign_key_graph.add_edge(
                            source_column.table.id,
                            dest_column.table.id,
                            columns=(source_column_id, dest_column_id))
                
                db_path = None
                if os.path.exists(os.path.join(self.db_dir, database_id, f"{database_id}.sqlite")):
                    db_path = os.path.join(self.db_dir, database_id, f"{database_id}.sqlite")
                elif os.path.exists(os.path.join(self.custom_db_dir, f"{database_id}.sqlite")):
                    db_path = os.path.join(self.custom_db_dir, f"{database_id}.sqlite")
                
                connection = None
                if db_path and os.path.exists(db_path):
                    try:
                        connection = sqlite3.connect(db_path)
                        connection.text_factory = lambda b: b.decode(errors="ignore")  
                    except Exception as e:
                        print(f"⚠️ cannot connect with {db_path}: {e}")
                        connection = None
                
                # Create a Schema object
                schema = Schema(
                    db_id=database_id,
                    tables=tables,
                    columns=columns, 
                    foreign_key_graph=foreign_key_graph,
                    orig=schema_dict,
                    connection=connection
                )
                
                # 3. preprocess_item
                preprocessed_data = self.preprocessor.preprocess_item(
                    item=item,
                    schema=schema,
                    validation_info=None,
                    section="test"
                )
                
                # 🔍 Debug: Output preprocessing results
                print("\n🔍 === Preprocessing Debug Info ===")
                print(f"📝 problem: {question}")
                print(f"🗃️ database: {database_id}")
                print(f"📊 preprocess data type: {type(preprocessed_data)}")
                if preprocessed_data:
                    print(f"📋 Preprocessing data key: {list(preprocessed_data.keys())}")
                    
                    if 'sc_link' in preprocessed_data:
                        sc_link = preprocessed_data['sc_link']
                        print(f"🔗 Schema Linking result: {sc_link}")
                        if sc_link:
                            print("  detailed Schema linking:")
                            for link_type, links in sc_link.items():
                                if links:
                                    print(f"    {link_type}: {links}")
                     
                    if 'cv_link' in preprocessed_data:
                        cv_link = preprocessed_data['cv_link']
                        print(f"📲 Result of cell Value Linking: {cv_link}")
                        if cv_link:
                            print("  detailed Cell linking:")
                            for link_type, links in cv_link.items():
                                if links:
                                    print(f"    {link_type}: {links}")
                    
                    if 'question_for_copying' in preprocessed_data:
                        tokens = preprocessed_data['question_for_copying']
                        print(f"📝 token result ({len(tokens)} tokens): {tokens}")
                    
                    # 显示表和列信息
                    if 'tables' in preprocessed_data:
                        print(f"📊 table name: {preprocessed_data['tables']}")
                    if 'columns' in preprocessed_data:
                        columns = preprocessed_data['columns'][:10] 
                        print(f"📋 column name: {columns}")
                        
                else:
                    print("⚠️ preprocessed data is empty")
                print("🔍 === Preprocessing Debug End ===\n")
                
            except Exception as prep_error:
                print(f"⚠️ DAIL-SQL preprocessing failed: {prep_error}, using simplified version")
                preprocessed_data = {}
            
            # Format the schema into text
            schema_text = self._format_schema_from_json(schema_dict)
            
            return {
                "tables": schema_dict,
                "schema_text": schema_text,
                "preprocessed": preprocessed_data,
                "linked_columns": preprocessed_data.get("column_linking", []),
                "linked_cells": preprocessed_data.get("cell_linking", [])
            }
            
        except Exception as e:
            print(f"❌ Preprocessing failed: {str(e)}")
            raise Exception(f"Preprocessing failed: {str(e)}")
    
    def _format_schema_from_json(self, schema_info: Dict) -> str:
        """Format the JSON schema information into SQL CREATE TABLE statements"""
        schema_lines = []
        
        table_names = schema_info['table_names_original']
        column_names = schema_info['column_names_original']
        column_types = schema_info['column_types']
        
        tables_columns = {}
        for i, (table_id, col_name) in enumerate(column_names):
            if table_id == -1:
                continue
            if table_id not in tables_columns:
                tables_columns[table_id] = []
            tables_columns[table_id].append((col_name, column_types[i]))
        
        for table_id, table_name in enumerate(table_names):
            if table_id in tables_columns:
                schema_lines.append(f"CREATE TABLE {table_name} (")
                
                cols = tables_columns[table_id]
                for j, (col_name, col_type) in enumerate(cols):
                    col_def = f"    {col_name} {col_type}"
                    if j < len(cols) - 1:
                        col_def += ","
                    schema_lines.append(col_def)
                
                schema_lines.append(");")
                schema_lines.append("")
        
        return "\n".join(schema_lines)
    
    def _format_schema(self, tables: List) -> str:
        """Format the database schema into text"""
        schema_lines = []
        for table in tables:
            table_name = table['table_name']
            schema_lines.append(f"CREATE TABLE {table_name} (")
            
            for col in table['column_names_original']:
                if col[0] == -1: 
                    continue
                col_name = col[1]
                schema_lines.append(f"    {col_name},")
                
            schema_lines.append(");")
            schema_lines.append("")
        
        return "\n".join(schema_lines)
    
    async def _build_prompt(self, question: str, database_id: str, k_shot: int, schema_info: Dict) -> Dict:
        """DAIL-SQL Prompt builder"""
        try:
            print(f"🔄 Build DAIL-SQL Prompt (k_shot={k_shot})...")
            
            if k_shot > 0:
                print(f"🔄 Prepare the few-shot selector: {self.selector_type}")
                if not self.data_loader:
                    print("🔄 Initialize the data loader for the selector example...")
                    self.data_loader = load_data(
                        data_type="spider", 
                        path_data=self.dataset_dir
                    )
                
                print(f"✅ The data loader is ready. The few-shot selection will be automatically carried out during the prompt construction process.")
            
            print("🔄 Construct a complete Prompt...")
            
            # 1. Create an instance of the Prompt class
            prompt_cls = prompt_factory(
                repr_type=self.repr_type,
                k_shot=k_shot, 
                example_format=self.example_type,
                selector_type=self.selector_type
            )
            prompt = prompt_cls(data=self.data_loader, tokenizer="gpt-3.5-turbo")
            
            # 2. Build the target format
            # Get correctly formatted tables (SqliteTable objects needed by DAIL-SQL)
            preprocessed_data = schema_info.get('preprocessed', {})
            
            # Get correctly formatted tables from data_loader
            tables = None
            if self.data_loader and hasattr(self.data_loader, 'get_tables'):
                try:
                    # Use DAIL-SQL's get_tables method to get SqliteTable objects
                    tables = self.data_loader.get_tables(database_id)
                    print(f"✅ Retrieved {len(tables)} tables from data_loader")
                except Exception as e:
                    print(f"⚠️ Cannot get tables from data_loader: {e}")
                    tables = None
            
            # If data_loader retrieval fails, get directly from database file
            if not tables:
                db_path = self._get_db_path(database_id)
                if db_path and os.path.exists(db_path):
                    tables = get_tables(db_path)
                    print(f"✅ Retrieved {len(tables)} tables from database file")
                else:
                    print(f"❌ The database file does not exist.: {db_path}")
                    tables = []
            
            target = {
                "question": question,
                "db_id": database_id,
                "tables": tables,
                "question_toks": preprocessed_data.get("question_toks", question.split()),
                "query": "SELECT ",
                "query_skeleton": "",  
                "question_pattern": preprocessed_data.get("question_pattern", ""),
                "sc_link": preprocessed_data.get("sc_link"),
                "cv_link": preprocessed_data.get("cv_link"), 
                "question_for_copying": preprocessed_data.get("question_for_copying"),
                "column_to_table": preprocessed_data.get("column_to_table"),
                "table_names_original": preprocessed_data.get("table_names_original"),
                "column_names_original": preprocessed_data.get("column_names_original")
            }
            
            target = {k: v for k, v in target.items() if v is not None}
            
            # 3. prompt generation
            question_format = prompt.format(
                target=target,
                max_seq_len=4096,
                max_ans_len=200,
                scope_factor=100,
                cross_domain=False
            )
            
            full_prompt = question_format["prompt"]
            
            # 🔍Debug: Display actually selected few-shot examples
            print("\n🔍 === Few-Shot Example Debug Info ===")
            selected_examples = []
            if hasattr(prompt, 'selected_examples') and prompt.selected_examples:
                selected_examples = prompt.selected_examples
                print(f"✅ Selected {len(selected_examples)} few-shot examples:")
                for i, example in enumerate(selected_examples):
                    print(f"\n📚 Example {i+1}:")
                    print(f"  🗃️ Database: {example.get('db_id', 'N/A')}")
                    print(f"  ❓ Question: {example.get('question', 'N/A')}")
                    print(f"  💾 SQL: {example.get('query', 'N/A')}")
                    if 'similarity' in example:
                        print(f"  📊 Similarity: {example.get('similarity', 'N/A')}")
            else:
                print("⚠️ No selected examples found or attribute missing")
                print(f"🔍 Has selected_examples attribute: {hasattr(prompt, 'selected_examples')}")
                if hasattr(prompt, 'selected_examples'):
                    print(f"🔍 selected_examples value: {getattr(prompt, 'selected_examples', None)}")
                    
            print(f"🔍 Question format fields: {list(question_format.keys())}")
            print("🔍 === Few-Shot Debug End ===\n")
            
            # 🔍Debug: Display final prompt
            print("🔍 === Final Prompt Debug ===")
            print("📝 Complete Prompt Content:")
            print("-" * 80)
            print(full_prompt)
            print("-" * 80)
            print(f"✅ Prompt length: {len(full_prompt)} characters")
            print("🔍 === Prompt Debug End ===\n")
            
            print(f"✅ Prompt built successfully - length: {len(full_prompt)} chars")
            
            return {
                "prompt": full_prompt,
                "examples": selected_examples,
                "schema": schema_info['schema_text'],
                "repr_type": self.repr_type,
                "example_type": self.example_type,
                "selector_type": self.selector_type,
                "n_examples": question_format.get("n_examples", len(selected_examples))
            }
            
        except Exception as e:
            import traceback
            print(f"❌ Prompt construction failed: {str(e)}")
            print('--- Detailed anomaly tracking ---')
            traceback.print_exc()
            for var_name, var_val in list(locals().items()):
                if var_val is None:
                    print(f"[Debug] Variable '{var_name}' is NoneType")
            print('--- Detailed anomaly tracking has been completed ---')
            return self._build_fallback_prompt(question, database_id, k_shot, schema_info)
    
    async def _execute_llm_with_validation(self, prompt_info: Dict, request: TextToSQLRequest, processing_steps: List[str]) -> List[SQLResult]:
        """Executing LLM call and SQL validation"""
        if not self.llm_initialized:
            raise Exception("The LM client has not been initialized. Please provide the OPENAI_API_KEY.")
        
        model = request.model if request.model is not None else "gpt-4"
        temperature = request.temperature if request.temperature is not None else 0.1
        max_retries = request.max_retries if request.max_retries is not None else 3
        
        sql_results = []
        
        try:
            validator = self._build_validator(request.database_id)
            
            for attempt in range(max_retries):
                processing_steps.append(f"🔄 LLM调用尝试 {attempt + 1}/{max_retries}")
                
                response = await self._call_llm(
                    prompt_info["prompt"], 
                    model, 
                    temperature
                )
                processing_steps.append(f"✅⚠️ Unable to obtain the sample information for the selected option: {response[:100]}...")
                
                # Extract SQL
                sql = self._extract_sql_from_response(response)
                if not sql:
                    processing_steps.append("❌ Failed to extract SQL from the response")
                    continue
                
                processing_steps.append(f"✅extract SQL: {sql}")
                
                # SQL validation
                processing_steps.append("🔄 Verify SQL syntax and execute")
                validation_result = validator.validate_comprehensive(sql)
                
                sql_result = SQLResult(
                    sql=sql,
                    validation_passed=validation_result.get("overall_passed", False),
                    errors=validation_result.get("all_errors", [])
                )
                
                if validation_result.get("overall_passed", False):
                    processing_steps.append("✅ SQL verification successful")
                else:
                    processing_steps.append(f"❌ SQL validation failed: {validation_result.get('all_errors', [])}")
                
                sql_results.append(sql_result)
                
                if sql_result.validation_passed:
                    break
            
            return sql_results
            
        except Exception as e:
            raise Exception(f"LLM execution failed: {str(e)}")
    
    def _build_validator(self, database_id: str) -> ImprovedSQLValidator:
        """Build an SQL validator (supporting pre-configured and customized databases)）""" 
        db_path = os.path.join(self.db_dir, database_id, f"{database_id}.sqlite")
        sql_path = os.path.join(self.db_dir, database_id, "schema.sql")
        
        if not os.path.exists(db_path):
            custom_db_path = os.path.join(self.custom_db_dir, f"{database_id}.sqlite")
            if os.path.exists(custom_db_path):
                db_path = custom_db_path
                sql_path = None 
        
        return ImprovedSQLValidator(db_path, sql_path if sql_path and os.path.exists(sql_path) else None)
    
    async def _call_llm(self, prompt: str, model: str, temperature: float) -> str:
        """Invoke LLM"""
        try:
            response = ask_llm(model, [prompt], temperature, 1)
            return response['response'][0]
            
        except Exception as e:
            raise Exception(f"LLM call failed: {str(e)}")
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Custom SQL extraction function to extract complete multiple rows of SQL"""
        try:
            # First, handle the repeated output.
            processed_response = process_duplication(response)
            
            # Method 1: Search for ```sql code block
            if "```sql" in processed_response:
                parts = processed_response.split("```sql")
                if len(parts) >= 2:
                    sql_part = parts[1].split("```")[0]
                    sql = sql_part.strip()
                    if sql:
                        # Check if the prefix "SELECT" needs to be added
                        if not sql.upper().startswith('SELECT'):
                            sql = f"SELECT {sql}"
                        return sql
            
            # Method 2: Search for the part starting with "SELECT" until the entire SQL statement is completed (supports multiple lines)
            lines = processed_response.strip().split('\n')
            sql_lines = []
            in_sql = False
            
            for line in lines:
                stripped = line.strip()
                
                if stripped.upper().startswith('SELECT'):
                    in_sql = True
                    sql_lines.append(stripped)
                elif in_sql:
                    if stripped and not stripped.startswith('Question:') and not stripped.startswith('Example'):
                        sql_lines.append(stripped)
                        if stripped.endswith(';'):
                            break
                    elif not stripped:
                        continue
                    else:
                        break
            
            if sql_lines:
                return ' '.join(sql_lines).strip()
            
            import re
            
            sql_pattern = r'([a-zA-Z_][a-zA-Z0-9_.]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_.]*)*\s+FROM\s+[a-zA-Z_][a-zA-Z0-9_.]*(?:\s+.*)?)'
            match = re.search(sql_pattern, processed_response, re.IGNORECASE | re.DOTALL)
            if match:
                sql_fragment = match.group(1).strip()
                full_sql = f"SELECT {sql_fragment}"
                print(f"🔧 Automatically add the SELECT prefix: {full_sql}")
                return full_sql
                
        except Exception as e:
            print(f"❌ SQL extraction failed: {e}, trying fallback method")
        
        return ""
    
    def _build_retry_prompt(self, original_prompt: str, sql: str, errors: List[str]) -> str:
        """Append error feedback to the prompt so Copilot can self-correct on the next attempt"""
        error_lines = "\n".join(f"  - {e}" for e in errors)
        correction_block = (
            f"\n\n-- The previous attempt produced the following SQL:\n"
            f"-- {sql}\n"
            f"-- which failed validation with these errors:\n"
            f"{error_lines}\n"
            f"-- Please generate a corrected SQL query that fixes these issues.\n"
            f"SQL:"
        )
        # Remove any trailing "SQL:" from the original prompt to avoid duplication
        trimmed = original_prompt.rstrip()
        if trimmed.endswith("SQL:"):
            trimmed = trimmed[:-4].rstrip()
        return trimmed + correction_block

    def _select_best_sql(self, sql_results: List[SQLResult], use_self_consistency: bool = False) -> str:
        """select the best SQL"""
        if not sql_results:
            return ""
        
        valid_sqls = [r for r in sql_results if r.validation_passed]
        if valid_sqls:
            return valid_sqls[0].sql
        
        return sql_results[0].sql
    
    def get_available_databases(self) -> List[DatabaseInfo]:
        """Get available database list (including preconfigured and user-uploaded databases)"""
        databases = []
        
        if os.path.exists(self.db_dir):
            tables_json_path = os.path.join(self.dataset_dir, "spider", "tables.json")
            if os.path.exists(tables_json_path):
                try:
                    with open(tables_json_path, 'r', encoding='utf-8') as f:
                        tables_data = json.load(f)
                    
                    for table_info in tables_data:
                        db_id = table_info['db_id']
                        table_names = [name for name in table_info['table_names_original']]
                        
                        sqlite_file = os.path.join(self.db_dir, db_id, f"{db_id}.sqlite")
                        if os.path.exists(sqlite_file):
                            databases.append(DatabaseInfo(
                                database_id=db_id,
                                tables=table_names,
                                description=f"Spider数据库: {db_id}"
                            ))
                except Exception as e:
                    print(f"❌ Failed to read Spider database info: {str(e)}")
        
        if os.path.exists(self.custom_tables_file):
            try:
                with open(self.custom_tables_file, 'r', encoding='utf-8') as f:
                    custom_tables_data = json.load(f)
                
                for table_info in custom_tables_data:
                    db_id = table_info['db_id']
                    table_names = [name for name in table_info['table_names_original']]
                    
                    sqlite_file = os.path.join(self.custom_db_dir, f"{db_id}.sqlite")
                    if os.path.exists(sqlite_file):
                        databases.append(DatabaseInfo(
                            database_id=db_id,
                            tables=table_names,
                            description=f"user upload database: {db_id}"
                        ))
            except Exception as e:
                print(f"❌ Failed to read custom database info: {str(e)}")
        
        return databases

# ================================
# Session Store (in-memory, keyed by session_id)
# ================================
import uuid

# Each entry: { "question": str, "database_id": str, "prompt": str }
_session_store: Dict[str, Dict] = {}


# ================================
# FastAPI
# ================================

app = FastAPI(
    title="DAIL-SQL API",
    description="Efficient Text-to-SQL API service, based on the DAIL-SQL method",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

processor = None

@app.on_event("startup")
async def startup_event():
    """Application startup event"""
    global processor
    try:
        processor = DAILSQLProcessor()
        print("🚀 DAIL-SQL API service startup complete")
    except Exception as e:
        print(f"❌ Service startup failed: {str(e)}")
        raise e

@app.post("/api/v1/text-to-sql", response_model=TextToSQLResponse)
async def text_to_sql(request: TextToSQLRequest):
    """Text-to-SQL主接口"""
    if not processor:
        raise HTTPException(status_code=503, detail="服务未就绪")
    
    return await processor.process_text_to_sql(request)


@app.post("/api/v1/generate-prompt", response_model=GeneratePromptResponse)
async def generate_prompt(request: GeneratePromptRequest):
    """
    Build the DAIL-SQL prompt and return it together with a session ID.
    The caller (VS Code plugin) passes the prompt to GitHub Copilot and
    sends the resulting SQL back via /api/v1/validate-sql.
    """
    if not processor:
        raise HTTPException(status_code=503, detail="Service not ready")

    try:
        # Verify database exists
        db_path = processor._get_db_path(request.database_id)
        if not db_path:
            raise HTTPException(status_code=400, detail=f"Database '{request.database_id}' not found")

        # Run preprocessing + prompt building
        schema_info = await processor._run_preprocessing(request.question, request.database_id)
        k_shot = request.k_shot if request.k_shot is not None else 3
        prompt_info = await processor._build_prompt(
            request.question, request.database_id, k_shot, schema_info
        )

        prompt_text = prompt_info["prompt"]
        session_id = str(uuid.uuid4())

        # Persist session state so validate-sql can locate context later
        _session_store[session_id] = {
            "question": request.question,
            "database_id": request.database_id,
            "original_prompt": prompt_text,
            "current_prompt": prompt_text,
        }

        return GeneratePromptResponse(
            success=True,
            session_id=session_id,
            prompt=prompt_text,
            database_id=request.database_id,
            question=request.question,
        )

    except HTTPException:
        raise
    except Exception as e:
        return GeneratePromptResponse(
            success=False,
            session_id="",
            prompt="",
            database_id=request.database_id,
            question=request.question,
            error=str(e),
        )


@app.post("/api/v1/validate-sql", response_model=ValidateSQLResponse)
async def validate_sql(request: ValidateSQLRequest):
    """
    Validate a SQL string produced by GitHub Copilot.
    If validation fails and retries remain, returns a corrective prompt
    (with error details appended) that the plugin can send back to Copilot.
    The session is cleaned up once validation passes or all attempts are exhausted.
    """
    if not processor:
        raise HTTPException(status_code=503, detail="Service not ready")

    session = _session_store.get(request.session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session '{request.session_id}' not found or expired")

    database_id = session["database_id"]
    attempts_remaining = max(0, request.max_attempts - request.attempt)

    try:
        validator = processor._build_validator(database_id)
        validation_result = validator.validate_comprehensive(request.sql)
        passed = validation_result.get("overall_passed", False)
        errors = validation_result.get("all_errors", [])

        if passed:
            # Clean up session on success
            _session_store.pop(request.session_id, None)
            return ValidateSQLResponse(
                valid=True,
                sql=request.sql,
                errors=[],
                next_prompt=None,
                attempts_remaining=0,
            )
        else:
            # Build a corrective prompt if retries remain
            next_prompt = None
            if attempts_remaining > 0:
                next_prompt = processor._build_retry_prompt(
                    session["current_prompt"], request.sql, errors
                )
                # Update stored prompt for potential subsequent retries
                session["current_prompt"] = next_prompt
            else:
                # No more retries – clean up
                _session_store.pop(request.session_id, None)

            return ValidateSQLResponse(
                valid=False,
                sql=request.sql,
                errors=errors,
                next_prompt=next_prompt,
                attempts_remaining=attempts_remaining,
            )

    except Exception as e:
        _session_store.pop(request.session_id, None)
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@app.post("/api/v1/upload-database", response_model=UploadDatabaseResponse)
async def upload_database(file: UploadFile = File(...), database_id: str = Form(...)):
    """
Upload the SQLite database file 
Parameters:
- file: SQLite database file
- database_id: Unique identifier for the database (letters, numbers, and underscores) 
Return:
- Upload results and database information
    """
    try:
        print(f"🔄 uploading database: {database_id}")
        
        if not database_id.replace('_', '').isalnum():
            return UploadDatabaseResponse(
                success=False,
                database_id=database_id,
                message="The database ID can only consist of letters, numbers and underscores",
                error="Invalid database_id format"
            )
        
        if not file.filename.endswith('.sqlite'):
            return UploadDatabaseResponse(
                success=False,
                database_id=database_id,
                message="support .sqlite file only",
                error="Unsupported file format"
            )
        
        existing_dbs = processor.get_available_databases()
        if any(db.database_id == database_id for db in existing_dbs):
            return UploadDatabaseResponse(
                success=False,
                database_id=database_id,
                message=f"The database ID '{database_id}' already exists. Please use a different name",
                error="Database ID already exists"
            )
        
        upload_path = os.path.join(processor.custom_db_dir, f"{database_id}.sqlite")
        
        with open(upload_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        try:
            test_conn = sqlite3.connect(upload_path)
            test_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            test_conn.close()
        except Exception as e:
            os.remove(upload_path)
            return UploadDatabaseResponse(
                success=False,
                database_id=database_id,
                message=f"invalid sqlite file: {str(e)}",
                error="Invalid SQLite file"
            )
        
        try:
            schema_dict = processor._extract_schema_from_sqlite(upload_path, database_id)
            
            custom_tables = []
            if os.path.exists(processor.custom_tables_file):
                with open(processor.custom_tables_file, 'r', encoding='utf-8') as f:
                    custom_tables = json.load(f)
            
            custom_tables.append(schema_dict)
            
            with open(processor.custom_tables_file, 'w', encoding='utf-8') as f:
                json.dump(custom_tables, f, indent=2, ensure_ascii=False)
            
            print(f"✅ database {database_id} upload successfully")
            
            return UploadDatabaseResponse(
                success=True,
                database_id=database_id,
                message=f"database '{database_id}' upload successfully",
                tables=schema_dict['table_names_original']
            )
            
        except Exception as e:
            if os.path.exists(upload_path):
                os.remove(upload_path)
            
            return UploadDatabaseResponse(
                success=False,
                database_id=database_id,
                message=f"Failed to process database file: {str(e)}",
                error="Database processing failed"
            )
    
    except Exception as e:
        return UploadDatabaseResponse(
            success=False,
            database_id=database_id or "unknown",
            message=f"Upload failed: {str(e)}",
            error="Upload failed"
        )


@app.get("/api/v1/databases", response_model=List[DatabaseInfo])
async def get_databases():
    """Get available database list"""
    if not processor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    return processor.get_available_databases()

@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Health check"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now()
    )

if __name__ == "__main__":
    # Run server
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )