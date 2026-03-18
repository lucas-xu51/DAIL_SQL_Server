#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os

import tiktoken

from transformers import LlamaTokenizer,AutoTokenizer

def count_tokens(text, model="gpt-3.5-turbo"):
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")
    
    tokens = encoding.encode(text)
    
    return len(tokens)


def count_tokens_from_file(file_path):

    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    return count_tokens(text)

def check_token(input_text)->bool:
    token_count = count_tokens(input_text)
    if token_count >=7000:
        return True
    else:   
        return False
