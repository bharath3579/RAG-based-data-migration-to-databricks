from rag.connector import retrieve_relevant_chunks


def retrieve_context(source_platform: str, target_platform: str, functions: list[str], top_k: int = 5):
    return retrieve_relevant_chunks(
        source_platform=source_platform,
        target_platform=target_platform,
        functions=functions,
        top_k=top_k,
    )
