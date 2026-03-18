"""
Enhanced Masked Cache Creator for DAIL-SQL
Creates precomputed embeddings cache with mask processing for EUCDISQUESTIONMASK compatibility

This script preserves the original EUCDISQUESTIONMASK functionality while providing cache performance:
1. Applies mask processing to training questions using schema linking data
2. Generates embeddings for masked questions
3. Stores embeddings cache for high-performance retrieval
4. Uses Euclidean distance for similarity calculation (preserving original behavior)

Usage:
    python create_masked_cache.py
"""

import json
import numpy as np
import os
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import time

def load_schema_linking_data(schema_linking_file):
    """Load schema linking data from JSONL file"""
    print(f"Loading schema linking data from {schema_linking_file}...")
    schema_data = {}
    
    with open(schema_linking_file, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc="Loading schema linking"):
            data = json.loads(line.strip())
            # Use db_id + question as key for matching
            key = f"{data['db_id']}||{data['raw_question']}"
            schema_data[key] = data
    
    print(f"Loaded {len(schema_data)} schema linking entries")
    return schema_data

def mask_question_with_schema_linking_data(question_tokens, sc_link, cv_link, mask_tag="<mask>", value_tag="<unk>"):
    """
    Apply mask processing to question tokens using schema linking information
    
    Args:
        question_tokens: List of question tokens
        sc_link: Schema linking data (q_col_match, q_tab_match)
        cv_link: Column value linking data (num_date_match, cell_match)
        mask_tag: Token for schema elements (columns/tables)
        value_tag: Token for values (numbers/cells)
    
    Returns:
        str: Masked question string
    """
    def mask_tokens(tokens, mask_ids, tag):
        """Replace tokens at specified positions with tag"""
        new_tokens = []
        for idx, token in enumerate(tokens):
            if idx in mask_ids:
                new_tokens.append(tag)
            else:
                new_tokens.append(token)
        return new_tokens
    
    # Extract value match positions (numbers, dates, cells)
    num_date_match_ids = []
    if 'num_date_match' in cv_link:
        num_date_match_ids = [int(match.split(',')[0]) for match in cv_link['num_date_match']]
    
    cell_match_ids = []
    if 'cell_match' in cv_link:
        cell_match_ids = [int(match.split(',')[0]) for match in cv_link['cell_match']]
    
    value_match_q_ids = num_date_match_ids + cell_match_ids
    
    # Apply value masking first
    masked_tokens = mask_tokens(question_tokens, value_match_q_ids, value_tag)
    
    # Extract schema match positions (columns, tables)
    q_col_match_ids = []
    if 'q_col_match' in sc_link:
        q_col_match_ids = [int(match.split(',')[0]) for match in sc_link['q_col_match']]
    
    q_tab_match_ids = []
    if 'q_tab_match' in sc_link:
        q_tab_match_ids = [int(match.split(',')[0]) for match in sc_link['q_tab_match']]
    
    schema_match_q_ids = q_col_match_ids + q_tab_match_ids
    
    # Apply schema masking
    masked_tokens = mask_tokens(masked_tokens, schema_match_q_ids, mask_tag)
    
    return " ".join(masked_tokens)

def create_masked_cache():
    """Create masked embeddings cache for training data"""
    print("=== Enhanced Masked Cache Creator for DAIL-SQL ===")
    print("Preserving EUCDISQUESTIONMASK functionality with caching performance\n")
    
    # File paths
    train_file = './dataset/spider/train_spider.json'
    schema_linking_file = './dataset/spider/enc/train_schema-linking.jsonl'
    output_dir = './vector_cache'
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Load training data
    print("Loading training data...")
    with open(train_file, 'r', encoding='utf-8') as f:
        train_data = json.load(f)
    print(f"Loaded {len(train_data)} training samples")
    
    # Load schema linking data
    schema_data = load_schema_linking_data(schema_linking_file)
    
    # Initialize sentence transformer (same model as original EUCDISQUESTIONMASK)
    print("\nInitializing sentence transformer...")
    model_name = "sentence-transformers/all-mpnet-base-v2"
    bert_model = SentenceTransformer(model_name, device="cpu")
    print(f"Using model: {model_name}")
    
    # Process training questions with mask
    print("\nProcessing training questions with mask...")
    masked_questions = []
    valid_indices = []  # Track which samples have schema linking data
    
    for idx, sample in enumerate(tqdm(train_data, desc="Applying masks")):
        db_id = sample.get('db_id', '')
        question = sample.get('question', '')
        
        # Create key to match schema linking data
        key = f"{db_id}||{question}"
        
        if key in schema_data:
            schema_info = schema_data[key]
            question_tokens = schema_info['question_for_copying']
            sc_link = schema_info['sc_link']
            cv_link = schema_info['cv_link']
            
            # Apply mask processing
            masked_question = mask_question_with_schema_linking_data(
                question_tokens, sc_link, cv_link, 
                mask_tag="<mask>", value_tag="<unk>"
            )
            masked_questions.append(masked_question)
            valid_indices.append(idx)
        else:
            # If no schema linking data, use original question (fallback)
            masked_questions.append(question)
            valid_indices.append(idx)
    
    print(f"Successfully processed {len(masked_questions)} masked questions")
    
    # Count samples with schema linking data
    schema_linked_count = 0
    for sample in train_data:
        key = f"{sample.get('db_id', '')}||{sample.get('question', '')}"
        if key in schema_data:
            schema_linked_count += 1
    
    print(f"Found schema linking for {schema_linked_count} samples")
    
    # Generate embeddings for masked questions
    print("\nGenerating embeddings for masked questions...")
    start_time = time.time()
    
    # Process in batches to avoid memory issues
    batch_size = 64
    all_embeddings = []
    
    for i in tqdm(range(0, len(masked_questions), batch_size), desc="Computing embeddings"):
        batch = masked_questions[i:i+batch_size]
        batch_embeddings = bert_model.encode(batch)
        all_embeddings.append(batch_embeddings)
    
    # Concatenate all embeddings
    embeddings = np.vstack(all_embeddings)
    print(f"Generated embeddings shape: {embeddings.shape}")
    
    embedding_time = time.time() - start_time
    print(f"Embedding generation took {embedding_time:.2f} seconds")
    
    # Prepare cache data
    cache_data = {
        'embeddings': embeddings,
        'masked_questions': masked_questions,
        'valid_indices': valid_indices,
        'model_name': model_name,
        'mask_token': '<mask>',
        'value_token': '<unk>',
        'distance_metric': 'euclidean',
        'creation_time': time.time(),
        'num_samples': len(masked_questions),
        'embedding_dim': embeddings.shape[1]
    }
    
    # Save embeddings cache
    print("\nSaving masked embeddings cache...")
    cache_file = os.path.join(output_dir, 'masked_embeddings_cache.npz')
    np.savez_compressed(cache_file, **cache_data)
    
    # Save masked questions as text file for inspection
    questions_file = os.path.join(output_dir, 'masked_questions.txt')
    with open(questions_file, 'w', encoding='utf-8') as f:
        for i, (question, masked) in enumerate(zip([train_data[idx]['question'] for idx in valid_indices], masked_questions)):
            f.write(f"Sample {i}:\n")
            f.write(f"Original: {question}\n")  
            f.write(f"Masked:   {masked}\n")
            f.write("-" * 80 + "\n")
    
    cache_size = os.path.getsize(cache_file) / (1024 * 1024)  # Size in MB
    
    print(f"\n=== Masked Cache Creation Complete ===")
    print(f"Cache file: {cache_file}")
    print(f"Cache size: {cache_size:.1f} MB")
    print(f"Samples processed: {len(masked_questions)}")
    print(f"Embedding dimensions: {embeddings.shape[1]}")
    print(f"Distance metric: Euclidean (preserving original EUCDISQUESTIONMASK behavior)")
    print(f"Mask tokens: <mask> (schema), <unk> (values)")
    print(f"Questions preview saved to: {questions_file}")
    
    return cache_file, cache_size

if __name__ == "__main__":
    cache_file, cache_size = create_masked_cache()
    print(f"\nMasked cache ready! Use 'MASKED_CACHED' selector for high-performance masked similarity.")