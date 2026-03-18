import numpy as np
import random
import os
import json

from utils.utils import sql2skeleton, jaccard_similarity
from utils.linking_utils.application import mask_question_with_schema_linking


print('>>> ExampleSelectorTemplate.py imported')

class BasicExampleSelector(object):
    def __init__(self, data, *args, **kwargs):
        print('>>> ExampleSelectorTemplate.__init__ called')
        self.data = data
        self.train_json = self.data.get_train_json()
        print('train_json is None:', self.train_json is None)
        if self.train_json is None:
            raise ValueError('训练数据加载失败，请检查数据文件！')
        self.db_ids = [d["db_id"] for d in self.train_json]
        self.train_questions = self.data.get_train_questions()


    def get_examples(self, question, num_example, cross_domain=False):
        pass

    def domain_mask(self, candidates: list, db_id):
        cross_domain_candidates = [candidates[i] for i in range(len(self.db_ids)) if self.db_ids[i] != db_id]
        return cross_domain_candidates

    def retrieve_index(self, indexes: list, db_id):
        cross_domain_indexes = [i for i in range(len(self.db_ids)) if self.db_ids[i] != db_id]
        retrieved_indexes = [cross_domain_indexes[i] for i in indexes]
        return retrieved_indexes


class RandomExampleSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)
        random.seed(0)

    def get_examples(self, target, num_example, cross_domain=False):
        train_json = self.train_json
        indexes = list(range(len(train_json)))
        if cross_domain:
            indexes = domain_mask(indexes, target["db_id"])
        selected_indexes = random.sample(indexes, num_example)
        if cross_domain:
            selected_indexes = retrieve_index(selected_indexes, target["db_id"])
        return [train_json[index] for index in selected_indexes]


class CosineSimilarExampleSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        # self.SELECT_MODEL = "sentence-transformers/bert-base-nli-mean-tokens"

        from sentence_transformers import SentenceTransformer
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(self.train_questions)

        
    def get_examples(self, target, num_example, cross_domain=False):
        target_embedding = self.bert_model.encode([target["question"]])
        # target_embedding = self.bert_model.embed_text([target["question"]]).cpu().detach().numpy()

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import cosine_similarity
        similarities = np.squeeze(cosine_similarity(target_embedding, self.train_embeddings)).tolist()
        pairs = [(similarity, index) for similarity, index in zip(similarities, range(len(similarities)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0], reverse=True)
        top_pairs = list()
        for s, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            if train_json[index]["question"] == target["question"]:
                continue
            top_pairs.append((index, s))
            if len(top_pairs) >= num_example:
                break

        return [train_json[index] for (index, s) in top_pairs]


class EuclideanDistanceExampleSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"

        from sentence_transformers import SentenceTransformer
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(self.train_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_embedding = self.bert_model.encode([target["question"]])

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        return [train_json[index] for (index, d) in top_pairs]


class EuclideanDistanceThresholdExampleSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        # self.top_distances = list()
        self.threshold = 0.85

        from sentence_transformers import SentenceTransformer
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(self.train_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_embedding = self.bert_model.encode([target["question"]])

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if (cross_domain and similar_db_id == target["db_id"]) or d > self.threshold:
                continue
            top_pairs.append((index, d))
            # self.top_distances.append(d)
            if len(top_pairs) >= num_example:
                break
        # print("mean", np.mean(self.top_distances))    # 0.822
        # print("std", np.std(self.top_distances, ddof=1))  # 0.144
        # print("max", max(self.top_distances)) # 1.166

        return [train_json[index] for (index, d) in top_pairs]


class EuclideanDistanceSkeletonSimilarThresholdSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        self.threshold = 0.85
        self.mask_token = "<mask>"  # the "<mask>" is the mask token of all-mpnet-base-v2
        self.value_token = "<unk>"  # the "<unk>" is the unknown token of all-mpnet-base-v2

        from sentence_transformers import SentenceTransformer
        train_mask_questions = mask_question_with_schema_linking(self.train_json, mask_tag=self.mask_token, value_tag=self.value_token)
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(train_mask_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_mask_question = mask_question_with_schema_linking([target], mask_tag=self.mask_token, value_tag=self.value_token)
        target_embedding = self.bert_model.encode(target_mask_question)

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            # Skeleton similarity
            if jaccard_similarity(train_json[index]["query_skeleton"], target["query_skeleton"]) < self.threshold:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        if len(top_pairs) < num_example:
            for d, index in pairs_sorted:
                similar_db_id = train_json[index]["db_id"]
                if cross_domain and similar_db_id == target["db_id"]:
                    continue
                # Skeleton similarity
                if jaccard_similarity(train_json[index]["query_skeleton"], target["query_skeleton"]) >= self.threshold:
                    continue
                top_pairs.append((index, d))
                if len(top_pairs) >= num_example:
                    break

        return [train_json[index] for (index, d) in top_pairs]


class EuclideanDistanceQuestionMaskSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        self.mask_token = "<mask>"  # the "<mask>" is the mask token of all-mpnet-base-v2
        self.value_token = "<unk>" # the "<unk>" is the unknown token of all-mpnet-base-v2

        from sentence_transformers import SentenceTransformer
        train_mask_questions = mask_question_with_schema_linking(self.train_json, mask_tag=self.mask_token, value_tag=self.value_token)
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(train_mask_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_mask_question = mask_question_with_schema_linking([target], mask_tag=self.mask_token, value_tag=self.value_token)
        target_embedding = self.bert_model.encode(target_mask_question)

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        return [train_json[index] for (index, d) in top_pairs]
    
    
class EuclideanDistancePreSkeletonSimilarThresholdSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        self.threshold = 0.85

        from sentence_transformers import SentenceTransformer
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(self.train_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_embedding = self.bert_model.encode([target["question"]])

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            # Skeleton similarity
            if jaccard_similarity(train_json[index]["pre_skeleton"], target["pre_skeleton"]) < self.threshold:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        if len(top_pairs) < num_example:
            for d, index in pairs_sorted:
                similar_db_id = train_json[index]["db_id"]
                if cross_domain and similar_db_id == target["db_id"]:
                    continue
                # Skeleton similarity
                if jaccard_similarity(train_json[index]["pre_skeleton"], target["pre_skeleton"]) >= self.threshold:
                    continue
                top_pairs.append((index, d))
                if len(top_pairs) >= num_example:
                    break

        return [train_json[index] for (index, d) in top_pairs]


class EuclideanDistancePreSkeletonSimilarPlusSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"

        from sentence_transformers import SentenceTransformer
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(self.train_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_embedding = self.bert_model.encode([target["question"]])

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        train_json = self.train_json
        for i in range(len(train_json)):
            distances[i] -= jaccard_similarity(train_json[i]["pre_skeleton"], target["pre_skeleton"])
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        return [train_json[index] for (index, d) in top_pairs]
    

class EuclideanDistanceQuestionMaskPreSkeletonSimilarThresholdSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        self.mask_token = "<mask>"  # the "<mask>" is the mask token of all-mpnet-base-v2
        self.value_token = "<unk>"  # the "<unk>" is the unknown token of all-mpnet-base-v2
        self.threshold = 0.85

        from sentence_transformers import SentenceTransformer
        train_mask_questions = mask_question_with_schema_linking(self.train_json, mask_tag=self.mask_token, value_tag=self.value_token)
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(train_mask_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_mask_question = mask_question_with_schema_linking([target], mask_tag=self.mask_token, value_tag=self.value_token)
        target_embedding = self.bert_model.encode(target_mask_question)

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            # Skeleton similarity
            if jaccard_similarity(train_json[index]["pre_skeleton"], target["pre_skeleton"]) < self.threshold:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        if len(top_pairs) < num_example:
            for d, index in pairs_sorted:
                similar_db_id = train_json[index]["db_id"]
                if cross_domain and similar_db_id == target["db_id"]:
                    continue
                # Skeleton similarity
                if jaccard_similarity(train_json[index]["pre_skeleton"], target["pre_skeleton"]) >= self.threshold:
                    continue
                top_pairs.append((index, d))
                if len(top_pairs) >= num_example:
                    break

        return [train_json[index] for (index, d) in top_pairs]


class EuclideanDistanceQuestionMaskPreSkeletonSimilarThresholdShiftSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        self.mask_token = "<mask>"  # the "<mask>" is the mask token of all-mpnet-base-v2
        self.value_token = "<unk>"  # the "<unk>" is the unknown token of all-mpnet-base-v2
        self.threshold = 0.85

        from sentence_transformers import SentenceTransformer
        train_mask_questions = mask_question_with_schema_linking(self.train_json, mask_tag=self.mask_token, value_tag=self.value_token)
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(train_mask_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_mask_question = mask_question_with_schema_linking([target], mask_tag=self.mask_token, value_tag=self.value_token)
        target_embedding = self.bert_model.encode(target_mask_question)

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            # Skeleton similarity
            if jaccard_similarity(train_json[index]["pre_skeleton"], target["pre_skeleton"]) < self.threshold:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        return [train_json[index] for (index, d) in top_pairs]



class SimpleCachedExampleSelector(object):
    """
    简化的例子选择器 - 使用预计算的embedding缓存
    避免复杂的get_train_json()预处理操作
    """
    
    def __init__(self, data, cache_dir='simple_cache', *args, **kwargs):
        print('>>> SimpleCachedExampleSelector.__init__ called')
        
        # 检查缓存是否存在
        import os
        if not os.path.exists(cache_dir):
            raise FileNotFoundError(f"缓存目录不存在: {cache_dir}，请先运行 create_simple_cache.py")
        
        self.cache_dir = cache_dir
        
        print("正在加载缓存的训练数据...")
        
        # 加载预计算的embeddings
        import numpy as np
        self.train_embeddings = np.load(f'{cache_dir}/training_embeddings.npy')
        
        # 加载training pairs
        import json
        with open(f'{cache_dir}/training_pairs.json', 'r', encoding='utf-8') as f:
            self.training_pairs = json.load(f)
        
        # 加载元数据
        with open(f'{cache_dir}/metadata.json', 'r', encoding='utf-8') as f:
            self.metadata = json.load(f)
        
        # 初始化模型（只用于input embedding）
        from sentence_transformers import SentenceTransformer
        self.bert_model = SentenceTransformer(self.metadata['model_name'], device="cpu")
        
        # 创建db_id到索引的映射
        self.db_ids = [pair['db_id'] for pair in self.training_pairs]
        
        print(f"缓存加载完成: {len(self.training_pairs)} 个训练样本")
    
    def get_examples(self, target, num_example, cross_domain=False):
        """
        获取最相似的例子
        
        Args:
            target: 目标问题字典，包含question和db_id
            num_example: 返回的例子数量
            cross_domain: 是否跨域选择
            
        Returns:
            list: 选择的训练样本列表
        """
        target_question = target.get('question', '')
        target_db_id = target.get('db_id', '')
        
        # 只计算input question的embedding
        input_embedding = self.bert_model.encode([target_question])
        
        # 计算与训练数据的相似度
        from sklearn.metrics.pairwise import cosine_similarity
        similarities = cosine_similarity(input_embedding, self.train_embeddings)[0]
        
        # 创建(相似度, 索引)对
        pairs = [(sim, idx) for idx, sim in enumerate(similarities)]
        pairs_sorted = sorted(pairs, key=lambda x: x[0], reverse=True)  # 降序排列
        
        top_pairs = []
        for sim, idx in pairs_sorted:
            similar_db_id = self.db_ids[idx]
            
            # 跨域过滤
            if cross_domain and similar_db_id == target_db_id:
                continue
            
            top_pairs.append((idx, sim))
            if len(top_pairs) >= num_example:
                break
        
        # 转换为DAIL-SQL期望的格式
        results = []
        for idx, sim in top_pairs:
            pair = self.training_pairs[idx]
            # 构造与原始train_json兼容的格式
            result = {
                'question': pair['question'],
                'query': pair['sql'],
                'db_id': pair['db_id'],
                'query_skeleton': pair['sql'],  # 简化版：直接使用SQL作为skeleton  
                'question_pattern': pair['question'],  # 简化版：直接使用原问题作为pattern
                'similarity': sim
            }
            results.append(result)
        
        return results
    
    def domain_mask(self, candidates: list, db_id):
        """兼容性方法"""
        cross_domain_candidates = [candidates[i] for i in range(len(self.db_ids)) if self.db_ids[i] != db_id]
        return cross_domain_candidates

    def retrieve_index(self, indexes: list, db_id):
        """兼容性方法"""
        cross_domain_indexes = [i for i in range(len(self.db_ids)) if self.db_ids[i] != db_id]
        retrieved_indexes = [cross_domain_indexes[i] for i in indexes]
        return retrieved_indexes


class MaskedCachedExampleSelector(object):
    """
    Enhanced Masked Cache Selector - Preserves EUCDISQUESTIONMASK behavior with caching performance
    
    Features:
    - Applies schema linking based mask processing to questions
    - Uses Euclidean distance for similarity (matching original EUCDISQUESTIONMASK)
    - Precomputed embeddings cache for high performance
    - Supports both <mask> (schema) and <unk> (value) token replacement
    - Uses runtime preprocessing output from target (no train schema-linking lookup)
    """
    
    def __init__(self, data, *args, **kwargs):        
        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        self.mask_token = "<mask>"   # Schema elements (columns, tables)
        self.value_token = "<unk>"   # Values (numbers, cells)
        
        # Load precomputed masked embeddings cache
        cache_file = './vector_cache/masked_embeddings_cache.npz'
        if not os.path.exists(cache_file):
            raise FileNotFoundError(
                f"Masked cache file not found: {cache_file}\n"
                f"Please run 'python create_masked_cache.py' first to generate the masked cache."
            )
        
        print(f"Loading masked embeddings cache from {cache_file}...")
        cache_data = np.load(cache_file, allow_pickle=True)
        
        # Load cache components
        self.train_embeddings = cache_data['embeddings']
        self.cached_masked_questions = cache_data['masked_questions'].tolist()
        self.valid_indices = cache_data['valid_indices'].tolist()
        self.model_name = str(cache_data['model_name'])
        self.distance_metric = str(cache_data['distance_metric'])
        
        print(f"Masked cache loaded: {len(self.cached_masked_questions)} samples, {self.train_embeddings.shape[1]}D embeddings")
        print(f"Distance metric: {self.distance_metric}")
        print(f"Mask tokens: {self.mask_token} (schema), {self.value_token} (values)")
        
        # Load training data for results
        train_file = './dataset/spider/train_spider.json'
        with open(train_file, 'r', encoding='utf-8') as f:
            self.train_json = json.load(f)
        
        # Initialize sentence transformer for query encoding
        from sentence_transformers import SentenceTransformer
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")

    def _mask_question_with_schema_linking(self, question_tokens, sc_link, cv_link):
        """Apply mask processing to question tokens using schema linking information"""
        def mask_tokens(tokens, mask_ids, tag):
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
        masked_tokens = mask_tokens(question_tokens, value_match_q_ids, self.value_token)
        
        # Extract schema match positions (columns, tables)
        q_col_match_ids = []
        if 'q_col_match' in sc_link:
            q_col_match_ids = [int(match.split(',')[0]) for match in sc_link['q_col_match']]
        
        q_tab_match_ids = []
        if 'q_tab_match' in sc_link:
            q_tab_match_ids = [int(match.split(',')[0]) for match in sc_link['q_tab_match']]
        
        schema_match_q_ids = q_col_match_ids + q_tab_match_ids
        
        # Apply schema masking
        masked_tokens = mask_tokens(masked_tokens, schema_match_q_ids, self.mask_token)
        
        return " ".join(masked_tokens)

    def _get_masked_question(self, target):
        """Get masked version of target question using runtime preprocessing fields in target"""
        question = target.get('question', '')
        question_tokens = target.get('question_for_copying')
        sc_link = target.get('sc_link')
        cv_link = target.get('cv_link')

        if question_tokens and sc_link is not None and cv_link is not None:
            masked_question = self._mask_question_with_schema_linking(
                question_tokens, sc_link, cv_link
            )
            return masked_question

        print("Warning: Missing runtime schema linking fields for masking; fallback to original question")
        return question

    def get_examples(self, target, num_example, cross_domain=False):
        # Apply mask processing to target question
        target_masked_question = self._get_masked_question(target)
        target_embedding = self.bert_model.encode([target_masked_question])
        
        # Calculate Euclidean distance with cached masked embeddings
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]
        
        pairs_sorted = sorted(pairs, key=lambda x: x[0])  # Sort by distance (ascending)
        top_pairs = list()
        
        for d, cache_index in pairs_sorted:
            # Map cache index to original training data index
            train_index = self.valid_indices[cache_index]
            similar_db_id = self.train_json[train_index]["db_id"]
            
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            if self.train_json[train_index]["question"] == target["question"]:
                continue
            
            # Add compatibility fields for prompt template
            self.train_json[train_index]["query_skeleton"] = self.train_json[train_index].get("query_skeleton", "")
            self.train_json[train_index]["question_pattern"] = self.train_json[train_index].get("question_pattern", "")
            
            top_pairs.append((train_index, d))
            if len(top_pairs) >= num_example:
                break
        
        return [self.train_json[index] for (index, d) in top_pairs]
    
    def domain_mask(self, candidates: list, db_id):
        """兼容性方法"""
        return candidates
    
    def retrieve_index(self, indexes: list, db_id):
        """兼容性方法"""
        return indexes