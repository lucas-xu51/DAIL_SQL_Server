import json
from transformers import GPT2Tokenizer

INPUT_JSON_FILE = "E:\\result\\filter_2\\schema_linking 3.5\\3\\0.5\\questions.json"

def count_real_tokens(input_file):
    try:
        tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
        
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        total = 0
        for i, q in enumerate(data["questions"], 1):
            prompt_text = q.get("prompt", "")
            tokens = len(tokenizer.encode(prompt_text))
            total += tokens
        return total
        
    except Exception as e:
        print("error")
        return 0

if __name__ == "__main__":
    total_tokens = count_real_tokens(INPUT_JSON_FILE)
    
    print(f"\n\ntotal tokens number: {total_tokens}")