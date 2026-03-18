"""
简单的训练数据预处理和embedding缓存系统
只提取question和SQL，计算embedding，保存结果
"""
import json
import numpy as np
from sentence_transformers import SentenceTransformer
import os
from tqdm import tqdm

def create_simple_cache():
    """创建简单的embedding缓存"""
    print("正在加载训练数据...")
    
    # 加载原始训练数据
    with open('dataset/spider/train_spider.json', 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    
    print(f"总共 {len(raw_data)} 条训练数据")
    
    # 提取简单的question-SQL对
    training_pairs = []
    questions = []
    
    for item in raw_data:
        question = item['question']
        sql = item['query']
        db_id = item['db_id']
        
        training_pairs.append({
            'question': question,
            'sql': sql,
            'db_id': db_id
        })
        questions.append(question)
    
    print("正在加载sentence-transformer模型...")
    model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')
    
    # 计算所有问题的embedding
    print("正在计算embedding...")
    embeddings = model.encode(questions, batch_size=32, show_progress_bar=True)
    
    # 保存embedding和数据
    cache_dir = 'simple_cache'
    os.makedirs(cache_dir, exist_ok=True)
    
    # 保存embeddings (numpy格式)
    print("保存embedding...")
    np.save(f'{cache_dir}/training_embeddings.npy', embeddings)
    
    # 保存question-SQL pairs (JSON格式)
    print("保存训练数据pairs...")
    with open(f'{cache_dir}/training_pairs.json', 'w', encoding='utf-8') as f:
        json.dump(training_pairs, f, ensure_ascii=False, indent=2)
    
    # 保存元数据
    metadata = {
        'total_examples': len(training_pairs),
        'embedding_dim': embeddings.shape[1],
        'model_name': 'sentence-transformers/all-mpnet-base-v2'
    }
    
    with open(f'{cache_dir}/metadata.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    print(f"缓存创建完成！")
    print(f"- Training pairs: {len(training_pairs)}")
    print(f"- Embedding shape: {embeddings.shape}")
    print(f"- 缓存目录: {cache_dir}")

if __name__ == "__main__":
    create_simple_cache()