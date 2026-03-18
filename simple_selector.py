"""
简化的例子选择器 - 使用预计算的embedding缓存
避免复杂的预处理操作
"""
import json
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import os

class SimpleCachedSelector:
    def __init__(self, cache_dir='simple_cache'):
        """使用缓存的embedding初始化选择器"""
        self.cache_dir = cache_dir
        
        print("正在加载缓存的训练数据...")
        
        # 加载预计算的embeddings
        self.train_embeddings = np.load(f'{cache_dir}/training_embeddings.npy')
        
        # 加载training pairs
        with open(f'{cache_dir}/training_pairs.json', 'r', encoding='utf-8') as f:
            self.training_pairs = json.load(f)
        
        # 加载元数据
        with open(f'{cache_dir}/metadata.json', 'r', encoding='utf-8') as f:
            self.metadata = json.load(f)
        
        # 初始化模型（只用于input embedding）
        self.model = SentenceTransformer(self.metadata['model_name'])
        
        print(f"缓存加载完成: {len(self.training_pairs)} 个训练样本")
    
    def get_examples(self, input_question, k=3):
        """
        获取最相似的k个例子
        
        Args:
            input_question: 用户输入的自然语言问题
            k: 返回的例子数量
            
        Returns:
            list: 最相似的k个 {question, SQL} 对
        """
        # 只计算input question的embedding
        input_embedding = self.model.encode([input_question])
        
        # 计算与训练数据的相似度
        similarities = cosine_similarity(input_embedding, self.train_embeddings)[0]
        
        # 找到最相似的k个索引
        top_k_indices = np.argsort(similarities)[-k:][::-1]  # 降序排列
        
        # 返回对应的examples
        examples = []
        for idx in top_k_indices:
            example = {
                'question': self.training_pairs[idx]['question'],
                'sql': self.training_pairs[idx]['sql'],
                'db_id': self.training_pairs[idx]['db_id'],
                'similarity': similarities[idx]
            }
            examples.append(example)
        
        return examples
    
    def get_cache_info(self):
        """返回缓存信息"""
        return {
            'total_training_examples': len(self.training_pairs),
            'embedding_dimension': self.train_embeddings.shape[1],
            'model_name': self.metadata['model_name'],
            'cache_size_mb': self.train_embeddings.nbytes / (1024 * 1024)
        }

def test_simple_selector():
    """测试简化的选择器"""
    # 检查缓存是否存在
    if not os.path.exists('simple_cache'):
        print("缓存不存在，请先运行 create_simple_cache.py")
        return
    
    # 初始化选择器
    selector = SimpleCachedSelector()
    
    # 显示缓存信息
    cache_info = selector.get_cache_info()
    print("缓存信息:")
    for key, value in cache_info.items():
        print(f"  {key}: {value}")
    
    # 测试几个问题
    test_questions = [
        "How many heads of departments are older than 56?",
        "What is the average age of all employees?",
        "List all students in the computer science department"
    ]
    
    print("\n测试相似度搜索:")
    for question in test_questions:
        print(f"\n输入问题: {question}")
        examples = selector.get_examples(question, k=3)
        
        for i, example in enumerate(examples, 1):
            print(f"  {i}. 相似度: {example['similarity']:.4f}")
            print(f"     问题: {example['question']}")
            print(f"     SQL: {example['sql'][:50]}...")

if __name__ == "__main__":
    test_simple_selector()