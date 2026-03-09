from functools import partial

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, StateGraph

from email_processor.agent.nodes import classify, load_body, summarize
from email_processor.agent.state import EmailAgentState
from email_processor.blob_store import BlobStore


def build_graph(blob_store: BlobStore, llm: BaseChatModel):
    graph = StateGraph(EmailAgentState)

    graph.add_node("load_body", partial(load_body, blob_store=blob_store))
    graph.add_node("classify", partial(classify, llm=llm))
    graph.add_node("summarize", partial(summarize, llm=llm))

    graph.set_entry_point("load_body")

    graph.add_edge("load_body", "classify")
    graph.add_edge("load_body", "summarize")
    graph.add_edge("classify", END)
    graph.add_edge("summarize", END)

    return graph.compile()
