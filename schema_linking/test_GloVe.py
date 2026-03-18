import numpy as np
import sys
from pathlib import Path

# 获取项目根目录（DAIL-SQL）
project_root = str(Path(__file__).resolve().parent.parent)
sys.path.append(project_root)

# 导入 GloVe 类
from utils.pretrained_embeddings import GloVe

def test_glove_similarity(word_pairs=None, top_n_similar=5):
    print("加载GloVe词向量...")
    word_emb = GloVe(kind='42B', lemmatize=True)  # 初始化GloVe
    print("加载完成")
    
    # 定义测试词对
    if word_pairs is None:
        word_pairs = [
            ("big", "large"), ("car", "automobile"), ("buy", "purchase"),
            ("happy", "glad"), ("fast", "quick"), ("apple", "banana"),
            ("apple", "car"), ("bank", "financial"), ("bank", "river")
        ]
    
    # 测试词语对相似度
    print("\n=== 词语对相似度测试 ===")
    for word1, word2 in word_pairs:
        try:
            # 使用 lookup 方法获取词向量（替换 get_word_vector）
            vec1 = word_emb.lookup(word1)
            vec2 = word_emb.lookup(word2)
            
            if vec1 is None or vec2 is None:
                print(f"警告: {word1} 或 {word2} 不在词汇表中")
                continue
            
            # 计算余弦相似度
            similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
            print(f"({word1}, {word2}) 相似度: {similarity:.4f}")
        except Exception as e:
            print(f"错误: 处理 {word1} 和 {word2} 时出错 - {str(e)}")
    
    # 测试最相似词（需手动实现，因为原类没有 find_similar_words 方法）
    print("\n=== 最相似词查找测试 ===")
    test_words = ["apple", "car", "happy", "bank", "computer"]
    for word in test_words:
        try:
            vec = word_emb.lookup(word)
            if vec is None:
                print(f"{word} 不在词汇表中")
                continue
            
            # 遍历词表计算相似度（注意：GloVe词表很大，此操作可能较慢）
            similarities = []
            for token in word_emb.glove.stoi:
                if token == word:
                    continue
                token_vec = word_emb.lookup(token)
                if token_vec is not None:
                    sim = np.dot(vec, token_vec) / (np.linalg.norm(vec) * np.linalg.norm(token_vec))
                    similarities.append((token, sim))
            
            # 排序并取前N个
            similarities.sort(key=lambda x: x[1], reverse=True)
            top_similar = similarities[:top_n_similar]
            
            print(f"\n与 '{word}' 最相似的 {top_n_similar} 个词:")
            for i, (token, sim) in enumerate(top_similar, 1):
                print(f"{i}. {token}: {sim:.4f}")
        except Exception as e:
            print(f"错误: 处理 {word} 时出错 - {str(e)}")

if __name__ == "__main__":
    test_glove_similarity()