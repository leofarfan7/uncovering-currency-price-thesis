from langchain.globals import set_verbose, set_debug
from langchain_ollama import ChatOllama

from utils.llm_prompts import detect_exchange_rate_prompt_es, extract_exchange_rate_prompt_es, correct_detection_es, \
    correct_extraction_es, reassurance_detection_es, reassurance_extraction_es
from utils.services import check_ollama, extract_json_response


class LLMProcessing:
    """
    Handles processing of articles using Large Language Models (LLMs) for detection and extraction
    of exchange rate mentions. Supports both local and remote modes.
    """

    def __init__(self, mode):
        """
        Initialize the LLMProcessing class.

        Args:
            mode (str): The mode of operation, either "local" or "remote".
        """
        self.mode = mode
        if self.mode == "local":
            self.fast_model = "llama3.1:8b"
            self.intelligent_model = "deepseek-r1:14b"
            if check_ollama() == 1:
                print("[main] Ollama is not running. Please start the Ollama service.")
                retry = input("[main] Do you want to retry? (y/n): ").lower()
                if retry == "y":
                    if check_ollama() == 1:
                        raise ConnectionError("[main] Ollama is still not running. Exiting program.")
                else:
                    raise ConnectionError("[main] Exiting program.")
            self.fast_llm = ChatOllama(model=self.fast_model, temperature=0)
            self.intelligent_llm = ChatOllama(model=self.intelligent_model, temperature=0)
            self.settings = None
            self.limits = None
        else:  # Mixed
            pass
        set_debug(False)
        set_verbose(False)

    def process_article(self, article, _mode, attempt=1, messages=None):
        """
        Process an article to detect or extract exchange rate information using LLMs.

        Args:
            article (dict): The article data containing 'title', 'timestamp', and 'content'.
            _mode (str): The operation mode, either "detect" or "extract".
            attempt (int, optional): The current attempt number for processing. Defaults to 1.
            messages (list, optional): The message history for the LLM. Defaults to None.

        Returns:
            bool or tuple or None:
                - For "detect" mode: Returns True/False if the parallel exchange rate is mentioned.
                - For "extract" mode: Returns a tuple (hint_type, quote) if extraction is successful, otherwise None.
        """
        if _mode == "detect":
            instructions = detect_exchange_rate_prompt_es
            correction = correct_detection_es
            reassurance = reassurance_detection_es
        else:
            instructions = extract_exchange_rate_prompt_es
            correction = correct_extraction_es
            reassurance = reassurance_extraction_es
        if attempt == 1:
            messages = [
                {
                    "role": "system",
                    "content": instructions
                },
                {
                    "role": "user",
                    "content": f"Titulo: {article['title']}\nDate:{article['timestamp']}\nContenido:\n{article['content']}"
                },
                {
                    "role": "system",
                    "content": reassurance
                }
            ]
        elif attempt > 3:
            # print("Exceeded")
            if _mode == "detect":
                return False
            else:
                return None
        if _mode == "detect":
            response = self.fast_llm.invoke(messages)
        else:
            response = self.intelligent_llm.invoke(messages)
        data = extract_json_response(response.content)
        if data is not None:  # Successfully extracted data
            if _mode == "detect":
                return data.get("mentions_parallel_exchange_rate", False)
            else:  # extract mode
                hint_type = data.get("hint_type", None)
                quote = data.get("quote", None)
                if quote is not None:
                    quote = float(quote)
                return hint_type, quote
        else:
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": correction})
            attempt += 1
            return self.process_article(article, _mode=_mode, attempt=attempt, messages=messages)