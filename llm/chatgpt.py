import json.decoder
import time
from utils.enums import LLM

# 尝试导入新版本OpenAI库
try:
    from openai import OpenAI
    NEW_OPENAI = True
except ImportError:
    import openai
    NEW_OPENAI = False

# global variable to hold the OpenAI client
client = None

def init_chatgpt(OPENAI_API_KEY, OPENAI_GROUP_ID, model):
    global client
    if NEW_OPENAI:
        # 新版本API (openai>=1.0.0)
        client = OpenAI(api_key=OPENAI_API_KEY)
    else:
        # 旧版本API (openai<1.0.0) 
        openai.api_key = OPENAI_API_KEY
        if OPENAI_GROUP_ID and OPENAI_GROUP_ID.strip():
            openai.organization = OPENAI_GROUP_ID


def ask_completion(model, batch, temperature):
    if NEW_OPENAI:
        # 新版本API
        response = client.completions.create(
            model=model,
            prompt=batch,
            temperature=temperature,
            max_tokens=2000,  # 增加到1000 tokens，避免SQL被截断
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            stop=[";"]
        )
        response_clean = [choice.text for choice in response.choices]
        return dict(
            response=response_clean,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens
        )
    else:
        # 旧版本API
        response = openai.Completion.create(
            model=model,
            prompt=batch,
            temperature=temperature,
            max_tokens=2000,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            stop=[";"]
        )
        response_clean = [_["text"] for _ in response.choices]
        return dict(
            response=response_clean,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens
        )


def ask_chat(model, messages: list, temperature, n):
    if NEW_OPENAI:
        # 新版本API
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=2000,  # 增加到1000 tokens，避免SQL被截断
            n=n
        )
        response_clean = [choice.message.content for choice in response.choices]
        if n == 1:
            response_clean = response_clean[0]
        return dict(
            response=response_clean,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens
        )
    else:
        # 旧版本API
        response = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=2000, 
            n=n
        )
        response_clean = [choice.message.content for choice in response.choices]
        if n == 1:
            response_clean = response_clean[0]
        return dict(
            response=response_clean,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens
        )


def ask_llm(model: str, batch: list, temperature: float, n: int):
    n_repeat = 0
    while True:
        try:
            if model in LLM.TASK_COMPLETIONS:
                assert n == 1
                response = ask_completion(model, batch, temperature)
            elif model in LLM.TASK_CHAT:
                assert len(batch) == 1, "batch must be 1 in this mode"
                messages = [{"role": "user", "content": batch[0]}]
                response = ask_chat(model, messages, temperature, n)
                response['response'] = [response['response']]
            break
        except Exception as e:
            # 处理不同版本的错误类型
            if NEW_OPENAI:
                # 新版本错误类型
                if "rate_limit" in str(e).lower():
                    n_repeat += 1
                    print(f"Repeat for the {n_repeat} times for RateLimitError")
                    time.sleep(1)
                elif "json" in str(e).lower():
                    n_repeat += 1
                    print(f"Repeat for the {n_repeat} times for JSONDecodeError")
                    time.sleep(1)
                else:
                    n_repeat += 1
                    print(f"Repeat for the {n_repeat} times for OpenAIError: {e}")
                    time.sleep(1)
            else:
                # 旧版本错误类型
                if hasattr(e, '__class__') and 'RateLimitError' in str(e.__class__):
                    n_repeat += 1
                    print(f"Repeat for the {n_repeat} times for RateLimitError")
                    time.sleep(1)
                elif isinstance(e, json.decoder.JSONDecodeError):
                    n_repeat += 1
                    print(f"Repeat for the {n_repeat} times for JSONDecodeError")
                    time.sleep(1)
                else:
                    n_repeat += 1
                    print(f"Repeat for the {n_repeat} times for Exception: {e}")
                    time.sleep(1)

    return response
