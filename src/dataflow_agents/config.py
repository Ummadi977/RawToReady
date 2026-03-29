from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # LLM providers — set whichever you have
    google_api_key: str = ""
    openai_api_key: str = ""
    anthropic_api_key: str = ""

    # Which model to use (must match the provider's model names)
    dataflow_model: str = "gemini-2.0-flash"

    # Pipeline options
    dataflow_output_dir: str = "./output"
    dataflow_log_level: str = "INFO"

    # Optional LangSmith tracing
    langsmith_api_key: str = ""
    langsmith_project: str = "dataflow-agents"
    langsmith_tracing: bool = False

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
