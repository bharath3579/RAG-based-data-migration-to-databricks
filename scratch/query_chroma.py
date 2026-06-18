import sys
import chromadb

def search_db(query: str):
    print(f"Loading ChromaDB...")
    client = chromadb.PersistentClient(path="rag/chroma_db")
    
    # Optional optimization: Hide the noisy chromadb logging
    import logging
    logging.getLogger("chromadb").setLevel(logging.ERROR)
    
    collection = client.get_collection(name="databricks_docs")
    
    print(f"\nSearching for: '{query}'")
    results = collection.query(
        query_texts=[query],
        n_results=5
    )
    
    docs = results['documents'][0]
    metadatas = results['metadatas'][0]
    distances = results['distances'][0]
    
    for i in range(len(docs)):
        meta = metadatas[i]
        dist = distances[i]
        print("-" * 60)
        print(f"[{i+1}] Distance: {dist:.4f} | Title: {meta.get('title')}")
        print(f"URL: {meta.get('url')}")
        print("...")
        # Print first 200 characters of the chunk safely for Windows PowerShell
        safe_text = docs[i][:300].replace('\n', ' ').encode('ascii', 'ignore').decode()
        print(safe_text)
        print("...")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        search_db(" ".join(sys.argv[1:]))
    else:
        print("Usage: python search_db.py <your search query>")
