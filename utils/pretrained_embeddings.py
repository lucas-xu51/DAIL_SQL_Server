import abc
import functools
import os
import time

import bpemb
import corenlp
import torch
import torchtext

import nltk
from nltk.stem import WordNetLemmatizer

# Ensure NLTK WordNet data is available for lemmatization
try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')

from utils.linking_utils import corenlp


class Embedder(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def tokenize(self, sentence):
        '''Given a string, return a list of tokens suitable for lookup.'''
        pass

    @abc.abstractmethod
    def untokenize(self, tokens):
        '''Undo tokenize.'''
        pass

    @abc.abstractmethod
    def lookup(self, token):
        '''Given a token, return a vector embedding if token is in vocabulary.
        If token is not in the vocabulary, then return None.'''
        pass

    @abc.abstractmethod
    def contains(self, token):
        pass

    @abc.abstractmethod
    def to(self, device):
        '''Transfer the pretrained embeddings to the given device.'''
        pass


class NLTKLemmatizer(Embedder):

    def __init__(self, lemmatize=True):
        self.lemmatize = lemmatize
        self.corenlp_annotators = ['tokenize', 'ssplit']
        self.lemmatizer = WordNetLemmatizer()

    def tokenize(self, text):
        ann = corenlp.annotate(text, self.corenlp_annotators)
        tokens = [tok.word.lower() for sent in ann.sentence for tok in sent.token]
        if self.lemmatize:
            return [self.lemmatizer.lemmatize(t) for t in tokens]
        return tokens

    def tokenize_for_copying(self, text):
        ann = corenlp.annotate(text, self.corenlp_annotators)
        text_for_copying = [tok.originalText.lower() for sent in ann.sentence for tok in sent.token]
        tokens = [tok.word.lower() for sent in ann.sentence for tok in sent.token]
        if self.lemmatize:
            tokens = [self.lemmatizer.lemmatize(t) for t in tokens]
        return tokens, text_for_copying

    def untokenize(self, tokens):
        return ' '.join(tokens)

    def lookup(self, token):
        return None

    def contains(self, token):
        return False

    def to(self, device):
        return self


class GloVe(Embedder):

    def __init__(self, kind, lemmatize=False):
        cache = os.path.join(os.environ.get('CACHE_DIR', os.getcwd()), 'vector_cache')
        self.glove = torchtext.vocab.GloVe(name=kind, cache=cache)
        self.dim = self.glove.dim
        self.vectors = self.glove.vectors
        self.lemmatize = lemmatize
        self.corenlp_annotators = ['tokenize', 'ssplit']
        if lemmatize:
            self.corenlp_annotators.append('lemma')

    def tokenize(self, text):
        ann = corenlp.annotate(text, self.corenlp_annotators)
        if self.lemmatize:
            return [tok.lemma.lower() for sent in ann.sentence for tok in sent.token]
        else:
            return [tok.word.lower() for sent in ann.sentence for tok in sent.token]

    def untokenize(self, tokens):
        return ' '.join(tokens)

    def lookup(self, token):
        if token is None or len(token.strip()) == 0:
            return None
        i = self.glove.stoi.get(token)
        if i is None:
            return None
        return self.vectors[i]

    def contains(self, token):
        return bool(token and token.strip()) and token in self.glove.stoi

    def to(self, device):
        self.vectors = self.vectors.to(device)
        return self

    @functools.lru_cache(maxsize=1024)
    def tokenize_for_copying(self, text):
        ann = corenlp.annotate(text, self.corenlp_annotators)
        text_for_copying = [tok.originalText.lower() for sent in ann.sentence for tok in sent.token]
        if self.lemmatize:
            text_tokens = [tok.lemma.lower() for sent in ann.sentence for tok in sent.token]
        else:
            text_tokens = [tok.word.lower() for sent in ann.sentence for tok in sent.token]
        return text_tokens, text_for_copying


class MiniLM(Embedder):
    """Lightweight embedding interface for schema linking.

    This wraps a SentenceTransformer model and provides the same methods
    needed by compute_schema_linking (lookup / contains).

    It is intentionally lightweight: it only encodes single tokens and caches
    results to avoid repeated tensor computations.
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2", device: str = "cpu"):
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(model_name, device=device)
        self._cache = {}

        # For compatibility with code paths that expect GloVe-like objects
        # (e.g., SpiderEncoderV2Preproc uses `word_emb.corenlp_annotators`, `lemmatize`, and may inspect `glove`)
        from types import SimpleNamespace

        self.corenlp_annotators = ["tokenize", "ssplit"]
        self.lemmatize = False

        # Provide a minimal glove-like interface for any code that inspects word_emb.glove
        self.glove = SimpleNamespace(stoi={}, vectors=None)

    def tokenize(self, sentence):
        # Not used by schema linking (only lookup/contains are used), but keep for interface.
        return sentence.split()

    def untokenize(self, tokens):
        return " ".join(tokens)

    def lookup(self, token):
        if token is None or len(token.strip()) == 0:
            return None
        if token in self._cache:
            return self._cache[token]
        emb = self.model.encode([token], convert_to_numpy=True)
        vec = emb[0] if len(emb) > 0 else None
        self._cache[token] = vec
        return vec

    def contains(self, token):
        # Always allow tokens to be compared; caching will handle missing ones.
        return bool(token and token.strip())

    def to(self, device):
        self.model.to(device)
        return self

    @functools.lru_cache(maxsize=1024)
    def tokenize(self, text):
        ann = corenlp.annotate(text, self.corenlp_annotators)
        if self.lemmatize:
            return [tok.lemma.lower() for sent in ann.sentence for tok in sent.token]
        else:
            return [tok.word.lower() for sent in ann.sentence for tok in sent.token]

    @functools.lru_cache(maxsize=1024)
    def tokenize_for_copying(self, text):
        ann = corenlp.annotate(text, self.corenlp_annotators)
        text_for_copying = [tok.originalText.lower() for sent in ann.sentence for tok in sent.token]
        if self.lemmatize:
            text = [tok.lemma.lower() for sent in ann.sentence for tok in sent.token]
        else:
            text = [tok.word.lower() for sent in ann.sentence for tok in sent.token]
        return text, text_for_copying

    def untokenize(self, tokens):
        return ' '.join(tokens)

    def lookup(self, token):
        i = self.glove.stoi.get(token)
        if i is None:
            return None
        return self.vectors[i]

    def contains(self, token):
        return token in self.glove.stoi

    def to(self, device):
        self.vectors = self.vectors.to(device)