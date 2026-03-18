import time
from tqdm import tqdm
from ollama import Client
from openai import OpenAI

client = ...
client2 =...


def collect_response3(model="llama3.1:8b-instruct-fp16", prompt="", max_tokens=None, max_retries=3, retry_delay=1, stop=None):
    system_message = "You are an good SQL coder, you can help user analysis databse and code correct SQL, user will give you many vaild information. You must trust user's prompts and follow prompts to code SQL or fix SQL."
    retry_count = 0
    model="llama3.1:8b-instruct-fp16"
    
    with open('prompt.txt', 'w') as f:
        f.write(prompt)
    
    while retry_count <= max_retries:
        try:
            messages = [
                {
                    'role': 'system',
                    'content': system_message
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ]
            
            options = {}
            if max_tokens is not None:
                options['options'] = {'num_predict': max_tokens}
            if stop is not None:
                if not isinstance(stop, list):
                    stop = [stop]  
                options['options'] = options.get('options', {})
                options['options']['stop'] = stop
            
            params = {
                'model': model,
                'messages': messages
            }
            params.update(options)
            
            response = client.chat(**params)
            return response['message']['content']
        
        except Exception as e:
            print(f"尝试 {retry_count+1}/{max_retries+1} 失败: {e}")
            time.sleep(retry_delay)
            retry_count += 1
    
    return "Cannot response"

def collect_response(model="qwen2.5-coder:latest", prompt="", max_tokens=None, max_retries=3, retry_delay=1, stop=None):
    system_message = "You are an good SQL coder, you can help user analysis databse and code correct SQL, user will give you many vaild information. You must trust user's prompts and follow prompts to code SQL or fix SQL."
    retry_count = 0
    model="qwen2.5-coder:latest"
    
    while retry_count <= max_retries:
        try:
            messages = [
                {
                    'role': 'system',
                    'content': system_message
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ]
            
            options = {}
            if max_tokens is not None:
                options['options'] = {'num_predict': max_tokens}
            if stop is not None:
                if not isinstance(stop, list):
                    stop = [stop]  
                options['options'] = options.get('options', {})
                options['options']['stop'] = stop

            params = {
                'model': model,
                'messages': messages
            }
            params.update(options)
            
            response = client2.chat(**params)
            return response['message']['content']
        
        except Exception as e:
            print(f"尝试 {retry_count+1}/{max_retries+1} 失败: {e}")
            time.sleep(retry_delay)
            retry_count += 1
    
    return None

client1 = OpenAI(
    base_url = ...
    api_key = ...
)


i = 0
tokens = 0
prompt_tokens = 0
completion_tokens = 0

def collect_response2(model="llama3.1:8b-instruct-fp16", prompt="", max_tokens=None, max_retries=3, retry_delay=1, stop=None, temperature=0.2):

    system_message = "You are a PostgreSQL experienced database expert, you can help user analysis databse and text correct SQL, user will give you many vaild information. You must trust user's prompts and follow prompts to code SQL or fix SQL."
    retry_count = 0
    global clients, i
    
    while retry_count <= max_retries:
    # if 1:
        try:
        # if 1:
            messages = [
                {
                    'role': 'system',
                    'content': system_message
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ]
            
            
            completion = client1.chat.completions.create(
              model="qwen/qwen2.5-coder-7b-instruct",
              messages=messages,
              temperature=0.9,
              top_p=0.65,
              max_tokens=1024,
              stream=False
            )
            
            full_response = ""
            current_call_prompt_tokens = 0
            current_call_completion_tokens = 0
            current_call_total_tokens = 0
            
            # Collect response content
            if completion.choices and len(completion.choices) > 0:
                message_content = completion.choices[0].message.content
                if message_content:
                    full_response = message_content

            if hasattr(completion, 'usage') and completion.usage is not None:
                current_call_prompt_tokens = completion.usage.prompt_tokens
                current_call_completion_tokens = completion.usage.completion_tokens
                current_call_total_tokens = completion.usage.total_tokens
            
            global tokens, prompt_tokens, completion_tokens
            prompt_tokens += current_call_prompt_tokens
            completion_tokens += current_call_completion_tokens
            tokens += current_call_total_tokens
            # After the stream completes
            print(f"Prompt tokens: {prompt_tokens}")
            print(f"Completion tokens: {completion_tokens}")
            print(f"Total tokens: {tokens}")

            return full_response
            
        except Exception as e:
            if "This model's maximum context length is" in str(e):
                # Handle the case where the model's context length is exceeded
                return "Model's maximum context length exceeded."
            print(f"Attempt {retry_count+1}/{max_retries+1} failed: {e}")
            time.sleep(retry_delay)
            retry_count += 1
        retry_count += 1
        
    
    return "Maximum retry count reached, unable to get response"

