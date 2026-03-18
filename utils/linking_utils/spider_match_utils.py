import re
import string
import collections
import numpy as np
from scipy.spatial.distance import cosine

import nltk.corpus

STOPWORDS = set(nltk.corpus.stopwords.words('english'))
PUNKS = set(a for a in string.punctuation)

CELL_EXACT_MATCH_FLAG = "EXACTMATCH"
CELL_PARTIAL_MATCH_FLAG = "PARTIALMATCH"
COL_PARTIAL_MATCH_FLAG = "CPM"
COL_EXACT_MATCH_FLAG = "CEM"
TAB_PARTIAL_MATCH_FLAG = "TPM"
TAB_EXACT_MATCH_FLAG = "TEM"
COL_SEMANTIC_MATCH_FLAG = "CSM"  # New addition: Column semantic matching tags
TAB_SEMANTIC_MATCH_FLAG = "TSM"  # New addition: Table semantic matching tag


# New addition: Calculate the cosine similarity of two word vectors
def vector_similarity(vec1, vec2):
    if vec1 is None or vec2 is None:
        return 0.0
    return 1 - cosine(vec1, vec2)

# schema linking, similar to IRNet
def compute_schema_linking(question, column, table, word_emb=None, semantic_threshold=0.5, 
                          verbose=False, use_meaning=False):
    def partial_match(x_list, y_list):
        x_str = " ".join(x_list)
        y_str = " ".join(y_list)
        if x_str in STOPWORDS or x_str in PUNKS:
            return False
        if re.match(rf"\b{re.escape(x_str)}\b", y_str):
            assert x_str in y_str
            return True
        return False

    def exact_match(x_list, y_list):
        return " ".join(x_list) == " ".join(y_list)

    q_col_match = dict()
    q_tab_match = dict()
    col_id2tokens = {col_id: col_item for col_id, col_item in enumerate(column) if col_id != 0}
    tab_id2tokens = {tab_id: tab_item for tab_id, tab_item in enumerate(table)}

    # 1. Word form matching (always executed)
    matched_q_ids_by_form = set()
    n = 5
    while n > 0:
        for i in range(len(question) - n + 1):
            n_gram_list = question[i:i + n]
            n_gram = " ".join(n_gram_list)
            if not n_gram.strip():
                continue
            
            # Column exact matching
            for col_id, col_tokens in col_id2tokens.items():
                if exact_match(n_gram_list, col_tokens):
                    for q_id in range(i, i + n):
                        q_col_match[f"{q_id},{col_id}"] = COL_EXACT_MATCH_FLAG
                        matched_q_ids_by_form.add(q_id)
            
            # Table exact matching
            for tab_id, tab_tokens in tab_id2tokens.items():
                if exact_match(n_gram_list, tab_tokens):
                    for q_id in range(i, i + n):
                        q_tab_match[f"{q_id},{tab_id}"] = TAB_EXACT_MATCH_FLAG
                        matched_q_ids_by_form.add(q_id)

            # Column partial matching
            for col_id, col_tokens in col_id2tokens.items():
                if partial_match(n_gram_list, col_tokens):
                    for q_id in range(i, i + n):
                        if f"{q_id},{col_id}" not in q_col_match:
                            q_col_match[f"{q_id},{col_id}"] = COL_PARTIAL_MATCH_FLAG
                            matched_q_ids_by_form.add(q_id)
            
            # Table partial matching
            for tab_id, tab_tokens in tab_id2tokens.items():
                if partial_match(n_gram_list, tab_tokens):
                    for q_id in range(i, i + n):
                        if f"{q_id},{tab_id}" not in q_tab_match:
                            q_tab_match[f"{q_id},{tab_id}"] = TAB_PARTIAL_MATCH_FLAG
                            matched_q_ids_by_form.add(q_id)
        n -= 1

    # 2. Semantic matching (executed only when use_meaning=True)
    if use_meaning and word_emb is not None:
        for q_id, q_token in enumerate(question):
            if q_id in matched_q_ids_by_form:
                continue
            if q_token in STOPWORDS or q_token in PUNKS:
                continue
                
            q_vector = word_emb.lookup(q_token)
            if q_vector is None:
                continue
                
            # Column semantic matching
            for col_id, col_tokens in col_id2tokens.items():
                token_sims = []
                for col_tok in col_tokens:
                    if word_emb.contains(col_tok):
                        col_vec = word_emb.lookup(col_tok)
                        sim = vector_similarity(q_vector, col_vec)
                        token_sims.append(sim)
                
                max_sim = max(token_sims, default=0.0)
                if max_sim >= semantic_threshold and f"{q_id},{col_id}" not in q_col_match:
                    q_col_match[f"{q_id},{col_id}"] = COL_SEMANTIC_MATCH_FLAG

            # Table semantic matching
            for tab_id, tab_tokens in tab_id2tokens.items():
                token_sims = []
                for tab_tok in tab_tokens:
                    if word_emb.contains(tab_tok):
                        tab_vec = word_emb.lookup(tab_tok)
                        sim = vector_similarity(q_vector, tab_vec)
                        token_sims.append(sim)
                
                max_sim = max(token_sims, default=0.0)
                if max_sim >= semantic_threshold and f"{q_id},{tab_id}" not in q_tab_match:
                    q_tab_match[f"{q_id},{tab_id}"] = TAB_SEMANTIC_MATCH_FLAG

    return {"q_col_match": q_col_match, "q_tab_match": q_tab_match}


def compute_cell_value_linking(tokens, schema):
    def isnumber(word):
        try:
            float(word)
            return True
        except:
            return False

    def db_word_partial_match(word, column, table, db_conn):
        cursor = db_conn.cursor()

        p_str = f"select {column} from {table} where {column} like '{word} %' or {column} like '% {word}' or " \
                f"{column} like '% {word} %' or {column} like '{word}'"
        try:
            cursor.execute(p_str)
            p_res = cursor.fetchall()
            if len(p_res) == 0:
                return False
            else:
                return p_res
        except Exception as e:
            return False

    def db_word_exact_match(word, column, table, db_conn):
        cursor = db_conn.cursor()

        p_str = f"select {column} from {table} where {column} like '{word}' or {column} like ' {word}' or " \
                f"{column} like '{word} ' or {column} like ' {word} '"
        try:
            cursor.execute(p_str)
            p_res = cursor.fetchall()
            if len(p_res) == 0:
                return False
            else:
                return p_res
        except Exception as e:
            return False

    num_date_match = {}
    cell_match = {}

    for col_id, column in enumerate(schema.columns):
        if col_id == 0:
            assert column.orig_name == "*"
            continue
        match_q_ids = []
        for q_id, word in enumerate(tokens):
            if len(word.strip()) == 0:
                continue
            if word in STOPWORDS or word in PUNKS:
                continue

            num_flag = isnumber(word)
            if num_flag:    # TODO refine the date and time match
                if column.type in ["number", "time"]:
                    num_date_match[f"{q_id},{col_id}"] = column.type.upper()
            else:
                ret = db_word_partial_match(word, column.orig_name, column.table.orig_name, schema.connection)
                if ret:
                    # print(word, ret)
                    match_q_ids.append(q_id)
        f = 0
        while f < len(match_q_ids):
            t = f + 1
            while t < len(match_q_ids) and match_q_ids[t] == match_q_ids[t - 1] + 1:
                t += 1
            q_f, q_t = match_q_ids[f], match_q_ids[t - 1] + 1
            words = [token for token in tokens[q_f: q_t]]
            ret = db_word_exact_match(' '.join(words), column.orig_name, column.table.orig_name, schema.connection)
            if ret:
                for q_id in range(q_f, q_t):
                    cell_match[f"{q_id},{col_id}"] = CELL_EXACT_MATCH_FLAG
            else:
                for q_id in range(q_f, q_t):
                    cell_match[f"{q_id},{col_id}"] = CELL_PARTIAL_MATCH_FLAG
            f = t

    cv_link = {"num_date_match": num_date_match, "cell_match": cell_match}
    return cv_link


def match_shift(q_col_match, q_tab_match, cell_match):
    q_id_to_match = collections.defaultdict(list)
    
    # Collect all matches, including semantic matches
    for match_key in q_col_match.keys():
        q_id, c_id = map(int, match_key.split(','))
        match_type = q_col_match[match_key]
        q_id_to_match[q_id].append((match_type, c_id))
    
    for match_key in q_tab_match.keys():
        q_id, t_id = map(int, match_key.split(','))
        match_type = q_tab_match[match_key]
        q_id_to_match[q_id].append((match_type, t_id))
    
    relevant_q_ids = list(q_id_to_match.keys())
    
    # Define the matching priority (the larger the value, the higher the priority)
    MATCH_PRIORITY = {
        COL_EXACT_MATCH_FLAG: 4,
        TAB_EXACT_MATCH_FLAG: 4,
        COL_PARTIAL_MATCH_FLAG: 3,
        TAB_PARTIAL_MATCH_FLAG: 3,
        COL_SEMANTIC_MATCH_FLAG: 2,  # The priority of semantic matching is lower than that of morphological matching
        TAB_SEMANTIC_MATCH_FLAG: 2
    }
    
    # Sort the matching of each question word by priority
    priority = []
    for q_id in q_id_to_match.keys():
        # Remove duplicates and sort by priority
        matches = list(set(q_id_to_match[q_id]))
        matches.sort(key=lambda x: MATCH_PRIORITY[x[0]], reverse=True)
        q_id_to_match[q_id] = matches
        priority.append((len(matches), q_id))
    
    # Sort by the number of matches (prioritize problem words with fewer matches)
    priority.sort()
    
    # The final selected matching result
    selected_matches = []
    new_q_col_match, new_q_tab_match = {}, {}
    
    for _, q_id in priority:
        current_matches = q_id_to_match[q_id]
        
        # If there is already a match, select the highest priority match that does not conflict with the selected match
        if set(selected_matches) & set(current_matches):
            compatible_matches = [m for m in current_matches if m in selected_matches]
            if compatible_matches:
                res = [compatible_matches[0]]  # Select the compatibility match with the highest priority
            else:
                res = []
        else:
            # Otherwise, select the match with the highest priority
            res = [current_matches[0]] if current_matches else []
        
        # Add to the selected match
        for match in res:
            match_type, c_t_id = match
            selected_matches.append(match)
            if match_type.startswith('COL'):
                new_q_col_match[f'{q_id},{c_t_id}'] = match_type
            else:
                new_q_tab_match[f'{q_id},{c_t_id}'] = match_type
    
    # Handle cell matching (consistent with the original logic)
    new_cell_match = {}
    for match_key in cell_match.keys():
        q_id = int(match_key.split(',')[0])
        if q_id in relevant_q_ids:
            continue
        new_cell_match[match_key] = cell_match[match_key]
    
    return new_q_col_match, new_q_tab_match, new_cell_match