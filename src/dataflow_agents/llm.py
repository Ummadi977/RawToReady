from langchain_core.language_models import BaseChatModel
from .config import Settings, settings as default_settings


def get_llm(cfg: Settings | None = None) -> BaseChatModel:
    """
    Return a LangChain chat model based on whichever API key is set.

    Priority: Google Gemini → OpenAI → Anthropic

    Set the model name via DATAFLOW_MODEL in your .env:
        Gemini:    gemini-2.5-flash  (default)
        OpenAI:    gpt-4o
        Anthropic: claude-sonnet-4-6
    """
    cfg = cfg or default_settings

    if cfg.google_api_key:
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(
            model=cfg.dataflow_model,
            google_api_key=cfg.google_api_key,
        )

    if cfg.openai_api_key:
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=cfg.dataflow_model,
            api_key=cfg.openai_api_key,
        )

    if cfg.anthropic_api_key:
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=cfg.dataflow_model,
            api_key=cfg.anthropic_api_key,
        )

    raise ValueError(
        "No LLM API key found. Set one of:\n"
        "  GOOGLE_API_KEY      (Gemini)\n"
        "  OPENAI_API_KEY      (OpenAI)\n"
        "  ANTHROPIC_API_KEY   (Anthropic)\n"
        "in your .env file."
    )
