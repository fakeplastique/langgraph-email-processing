from functools import partial

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, StateGraph

from email_processor.agent.nodes import (
    ClassificationOutput,
    SummaryOutput,
    classify,
    load_body,
    make_llm_retry,
    summarize,
)
from email_processor.agent.state import EmailAgentState
from email_processor.blob_store import BlobStore


def build_graph(blob_store: BlobStore, llm: BaseChatModel, llm_retry_kwargs: dict | None = None):
    llm_retry = make_llm_retry(**(llm_retry_kwargs or {}))
    classify_invoke = llm_retry(llm.with_structured_output(ClassificationOutput).bind(max_tokens=256).invoke)
    summarize_invoke = llm_retry(llm.with_structured_output(SummaryOutput).bind(max_tokens=256).invoke)

    graph = StateGraph(EmailAgentState)

    graph.add_node("load_body", partial(load_body, blob_store=blob_store))
    graph.add_node("classify", partial(classify, llm_invoke=classify_invoke))
    graph.add_node("summarize", partial(summarize, llm_invoke=summarize_invoke))

    graph.set_entry_point("load_body")

    graph.add_edge("load_body", "classify")
    graph.add_edge("load_body", "summarize")
    graph.add_edge("classify", END)
    graph.add_edge("summarize", END)

    return graph.compile()
