# chaz 02.18.2026
# File Name: ollama_cache_manager.py
# requirements: make sure you install the following by entering the command in your terminal: pip install ollama redis
# Note: 1. Be sure you have the redis server running, run the command in a separate terminal: redis-server
#       2. Ollama desktop client or run the command in a speparate terminal: ollama serve.
#       3. Load the module llama3, run the command: ollama pull llama3 in the terminal, before running this script.
#       4. This script will alert the user wether the response was first time or cache was used.
#       5. To support cut and pasting and multi-lines be sure to enter the command in your terminal: pip install prompt_toolkit

import ollama
import redis
import hashlib
from datetime import datetime
from prompt_toolkit import prompt

class CacheManager:
    def __init__(self, host='localhost', port=6379, db=0, ttl=3600):
        try:
            self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            self.client.ping()
            self.enabled = True
        except:
            self.enabled = False
            print("--- [Redis Offline: Caching Disabled] ---")
        self.ttl = ttl

    def generate_key(self, model, prompt):
        key_data = f"{model}:{prompt.strip().lower()}"
        return hashlib.sha256(key_data.encode()).hexdigest()

    def get_cached_response(self, key):
        return self.client.get(key) if self.enabled else None

    def set_cache(self, key, response):
        if self.enabled: self.client.setex(key, self.ttl, response)

class OllamaChat:
    def __init__(self, model='llama3', cache_manager=None):
        self.model = model
        self.cache = cache_manager
        # Tell the AI what today's date is
        self.system_context = f"You are a helpful assistant. Today's date is {datetime.now().strftime('%B %d, %Y')}. "

    def ask(self, prompt):
        # We combine context + prompt for the AI, but maybe just prompt for the cache key
        full_prompt = self.system_context + prompt

        if self.cache:
            key = self.cache.generate_key(self.model, prompt)
            cached_val = self.cache.get_cached_response(key)
            if cached_val:
                return cached_val, True

        try:
            response = ollama.generate(model=self.model, prompt=full_prompt)
            result = response['response']
            if self.cache:
                key = self.cache.generate_key(self.model, prompt)
                self.cache.set_cache(key, result)
            return result, False
        except Exception as e:
            return f"Error: {str(e)}", False

if __name__ == "__main__":
    MODEL = 'llama3' # Ensure this matches your 'ollama list'
    cm = CacheManager(ttl=600)
    bot = OllamaChat(model=MODEL, cache_manager=cm)

    print(f"--- NFL Research Bot (Model: {MODEL}) ---")
    print("Today is:", datetime.now().strftime('%B %d, %Y'))
    print("Type 'exit' to quit.\n")

    while True:
        # prompt() handles system-level paste buffers much better than input()
        user_input = prompt("You (Multi-line paste supported): ")

        if user_input.lower() in ['exit', 'quit']:
            break
        if not user_input.strip():
            continue

        # 2. Pass that SAME variable to the bot
        answer, was_cached = bot.ask(user_input)

        status = "[⚡ CACHED]" if was_cached else "[🆕 NEW]"
        print(f"\nOllama {status}: {answer}\n")
