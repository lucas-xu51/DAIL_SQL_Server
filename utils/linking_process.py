import collections
import itertools
import json
import os

import attr
import numpy as np
import torch

from utils.linking_utils import abstract_preproc, corenlp, serialization
from utils.linking_utils.spider_match_utils import (
    compute_schema_linking,
    compute_cell_value_linking,
    match_shift
)

@attr.s
class PreprocessedSchema:
    column_names = attr.ib(factory=list)
    original_column_names = attr.ib(factory=list) # Original column name
    table_names = attr.ib(factory=list)
    table_bounds = attr.ib(factory=list)
    column_to_table = attr.ib(factory=dict)
    table_to_columns = attr.ib(factory=dict)
    foreign_keys = attr.ib(factory=dict)
    foreign_keys_tables = attr.ib(factory=lambda: collections.defaultdict(set))
    primary_keys = attr.ib(factory=list)

    # Only for BERT version
    normalized_column_names = attr.ib(factory=list)
    normalized_table_names = attr.ib(factory=list)


def preprocess_schema_uncached(schema,
                               tokenize_func,
                               include_table_name_in_column,
                               fix_issue_16_primary_keys,
                               bert=False):
    """If it is the bert version, the normalized version of the cache issue/column/table is used for schema linking"""
    r = PreprocessedSchema()

    if bert: assert not include_table_name_in_column

    last_table_id = None
    for i, column in enumerate(schema.columns):
        col_toks = tokenize_func(
            column.name, column.unsplit_name)

        type_tok = f'<type: {column.type}>'
        if bert:
            column_name = col_toks + [type_tok]
            r.normalized_column_names.append(Bertokens(col_toks))
        else:
            column_name = [type_tok] + col_toks

        if include_table_name_in_column:
            if column.table is None:
                table_name = ['<any-table>']
            else:
                table_name = tokenize_func(
                    column.table.name, column.table.unsplit_name)
            column_name += ['<table-sep>'] + table_name
        r.column_names.append(column_name)

        original_col_name = column.orig_name  
        r.original_column_names.append(original_col_name)

        table_id = None if column.table is None else column.table.id
        r.column_to_table[str(i)] = table_id
        if table_id is not None:
            columns = r.table_to_columns.setdefault(str(table_id), [])
            columns.append(i)
        if last_table_id != table_id:
            r.table_bounds.append(i)
            last_table_id = table_id

        if column.foreign_key_for is not None:
            r.foreign_keys[str(column.id)] = column.foreign_key_for.id
            r.foreign_keys_tables[str(column.table.id)].add(column.foreign_key_for.table.id)

    r.table_bounds.append(len(schema.columns))
    assert len(r.table_bounds) == len(schema.tables) + 1

    for i, table in enumerate(schema.tables):
        table_toks = tokenize_func(
            table.name, table.unsplit_name)
        r.table_names.append(table_toks)
        if bert:
            r.normalized_table_names.append(Bertokens(table_toks))
    last_table = schema.tables[-1]

    r.foreign_keys_tables = serialization.to_dict_with_sorted_values(r.foreign_keys_tables)
    r.primary_keys = [
        column.id
        for table in schema.tables
        for column in table.primary_keys
    ] if fix_issue_16_primary_keys else [
        column.id
        for column in last_table.primary_keys
        for table in schema.tables
    ]

    return r


class SpiderEncoderV2Preproc(abstract_preproc.AbstractPreproc):

    def __init__(
            self,
            save_path,
            min_freq=3,
            max_count=5000,
            include_table_name_in_column=True,
            word_emb=None,
            fix_issue_16_primary_keys=False,
            compute_sc_link=False,
            compute_cv_link=False,
            use_meaning=False,
            compress=False,
            compressed_data=None):
        if word_emb is None:
            self.word_emb = None
        else:
            self.word_emb = word_emb

        self.data_dir = os.path.join(save_path, 'enc')
        self.include_table_name_in_column = include_table_name_in_column
        self.fix_issue_16_primary_keys = fix_issue_16_primary_keys
        self.compute_sc_link = compute_sc_link
        self.compute_cv_link = compute_cv_link
        self.texts = collections.defaultdict(list)
        self.use_meaning = use_meaning 
        self.preprocessed_schemas = {}

        # New addition: Parameters related to compression mode
        self.compress = compress  # Whether to enable the compression mode
        self.compressed_data = compressed_data or {}  # Store compressed data {section: {index: data}}
        self.current_index = 0  # Used to track the index of the currently processed item
        self.debug_count = 0  # Debug the counter, used to identify the entries for processing


    def validate_item(self, item, schema, section):
        return True, None


    def add_item(self, item, schema, section, validation_info):
        # Record the currently processed index to obtain the corresponding compressed data
        self.current_index = len(self.texts[section])
        self.debug_count += 1  # Increment debug counter
        preprocessed = self.preprocess_item(item, schema, validation_info, section)
        self.texts[section].append(preprocessed)


    def clear_items(self):
        self.texts = collections.defaultdict(list)
        self.current_index = 0  # Reset the index
        self.debug_count = 0  # Reset the debug counter


    def preprocess_item(self, item, schema, validation_info, section):
        # 1. Extract the original data (for the final output)
        original_question = item['question']
        original_question_toks = item['question_toks'].copy()

        # 2. Determine the problem used for calculation (compression or raw)
        compressed_ids = None
        compressed_length = 0
        if self.compress and section in self.compressed_data and self.current_index in self.compressed_data[section]:
            # Compression mode: Calculate schema linking using compressed data
            compressed = self.compressed_data[section][self.current_index]
            compute_question = " ".join(compressed["columns"])
            compute_question_toks = compressed["columns"].copy()
            compressed_ids = compressed["ids"]
            compressed_length = len(compute_question_toks)
            
        else:
            # Non-compressed mode: Calculate using raw data
            compute_question = original_question
            compute_question_toks = original_question_toks

        # 3. Generate tokens based on computational data
        question, _ = self._tokenize_for_copying(compute_question_toks, compute_question)
        _, question_for_copying = self._tokenize_for_copying(original_question_toks, original_question)

        # Debug output for lemmatization / tokenization check
        print(f"[DEBUG] original_question={original_question}")
        print(f"[DEBUG] question={question}")
        print(f"[DEBUG] question_for_copying={question_for_copying}")
        print(f"[DEBUG] use_meaning={self.use_meaning}, word_emb={type(self.word_emb).__name__}")

        # 4. handle schema
        preproc_schema = self._preprocess_schema(schema)

        # 5. Calculate schema linking (using compressed/raw data)
        sc_link = {}
        if self.compute_sc_link:
            assert preproc_schema.column_names[0][0].startswith("<type:")
            column_names_without_types = [col[1:] for col in preproc_schema.column_names]
            sc_link = compute_schema_linking(
                question, 
                column_names_without_types, 
                preproc_schema.table_names,
                word_emb=self.word_emb,
                semantic_threshold=0.5,
                verbose=True,
                use_meaning=self.use_meaning
            )
            
            
            # If it is in compressed mode, perform index mapping and debugging
            if self.compress and compressed_ids is not None:
                sc_link = self._map_compressed_indices(sc_link, compressed_ids, compressed_length)

        else:
            sc_link = {"q_col_match": {}, "q_tab_match": {}}

        # 6. Calculate cell value linking (using compressed/raw data)
        cv_link = {}
        if self.compute_cv_link:
            cv_link = compute_cell_value_linking(question, schema)
            
            
            # If it is in compressed mode, perform index mapping and debugging
            if self.compress and compressed_ids is not None:
                cv_link = self._map_compressed_indices(cv_link, compressed_ids, compressed_length)
        else:
            cv_link = {"num_date_match": {}, "cell_match": {}}

        # 7. The construction return result: linking uses computational data, while others use raw data
        return {
            'raw_question': original_question,  # primal problem
            'db_id': schema.db_id,
            'question': original_question,  # Original problem (uncompressed
            'question_toks': original_question_toks,  # Original token (uncompressed
            'question_for_copying': question_for_copying,  # Computational token (possibly compressed)
            'sc_link': sc_link,  # Results based on compressed/original data
            'cv_link': cv_link,  # Results based on compressed/original data
            'columns': preproc_schema.column_names,
            'original_columns': preproc_schema.original_column_names,
            'tables': preproc_schema.table_names,
            'table_bounds': preproc_schema.table_bounds,
            'column_to_table': preproc_schema.column_to_table,
            'table_to_columns': preproc_schema.table_to_columns,
            'foreign_keys': preproc_schema.foreign_keys,
            'foreign_keys_table': preproc_schema.foreign_keys_tables,
            'primary_key': preproc_schema.primary_keys,
        }


    def _map_compressed_indices(self, temp_link, compressed_ids, compressed_length):
        """带详细调试输出的正确索引映射方法"""
        mapped_link = {"q_col_match": {}, "q_tab_match": {}}

        for key in ["q_col_match", "q_tab_match"]:
            if key not in temp_link:
                continue
                
            for indices_str, match_type in temp_link[key].items():
                try:
                    # partitioned index
                    parts = indices_str.split(',')
                    if len(parts) != 2:
                        print(f"[DEBUG] Format error. Skip non-standard matches: {indices_str}")
                        continue
                    
                    compressed_q_id = int(parts[0])
                    original_c_t_id = int(parts[1])
                    

                    # Only map the index of the problem token
                    if 0 <= compressed_q_id < len(compressed_ids):
                        original_q_id = compressed_ids[compressed_q_id]
                        mapped_key = f"{original_q_id},{original_c_t_id}"
                        
                        
                        mapped_link[key][mapped_key] = match_type
                        
                except Exception as e:
                    print(f"[ERROR] handle {indices_str} error: {str(e)}")
                    import traceback
                    traceback.print_exc()
        
        return mapped_link


    def _preprocess_schema(self, schema):
        if schema.db_id in self.preprocessed_schemas:
            return self.preprocessed_schemas[schema.db_id]
        result = preprocess_schema_uncached(schema, self._tokenize,
                                            self.include_table_name_in_column, self.fix_issue_16_primary_keys)
        self.preprocessed_schemas[schema.db_id] = result
        return result


    def _tokenize(self, presplit, unsplit):
        if self.word_emb:
            return self.word_emb.tokenize(unsplit)
        return presplit


    def _tokenize_for_copying(self, presplit, unsplit):
        if self.word_emb:
            return self.word_emb.tokenize_for_copying(unsplit)
        return presplit, presplit


    def save(self):
        os.makedirs(self.data_dir, exist_ok=True)
        for section, texts in self.texts.items():
            with open(os.path.join(self.data_dir, section + '_schema-linking.jsonl'), 'w') as f:
                for text in texts:
                    f.write(json.dumps(text) + '\n')


    def load(self, sections):
        for section in sections:
            self.texts[section] = []
            with open(os.path.join(self.data_dir, section + '_schema-linking.jsonl'), 'r') as f:
                for line in f.readlines():
                    if line.strip():
                        self.texts[section].append(json.loads(line))


    def dataset(self, section):
        return [
            json.loads(line)
            for line in open(os.path.join(self.data_dir, section + '.jsonl'))]
    