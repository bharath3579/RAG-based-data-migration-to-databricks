import sys
import urllib.request
import hashlib
from bs4 import BeautifulSoup
import chromadb

def clean_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, 'html.parser')
    for element in soup(["script", "style", "nav", "footer", "header", "aside"]):
        element.extract()
    main = soup.find('main') or soup.find('article') or soup.find('body')
    if not main: return ""
    text = main.get_text(separator='\n')
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return '\n'.join(lines)

def chunk_text(text: str, chunk_size=1000, overlap=200):
    chunks = []
    start = 0
    text_len = len(text)
    while start < text_len:
        end = start + chunk_size
        if end < text_len:
            last_newline = text.rfind('\n', start, end)
            if last_newline != -1 and last_newline > start + (chunk_size // 2):
                end = last_newline + 1
            else:
                last_period = text.rfind('. ', start, end)
                if last_period != -1 and last_period > start + (chunk_size // 2):
                    end = last_period + 2
        chunk = text[start:end].strip()
        if chunk: chunks.append(chunk)
        start = end - overlap
        if start >= text_len: break
    return chunks

def add_url(url: str, title: str):
    print(f"Fetching {url}...")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as response:
        html = response.read().decode('utf-8')
        
    text = clean_html(html)
    chunks = chunk_text(text)
    
    print(f"Opening ChromaDB...")
    client = chromadb.PersistentClient(path="rag/chroma_db")
    collection = client.get_collection(name="databricks_docs")
    
    documents = []
    metadatas = []
    ids = []
    
    for idx, chunk in enumerate(chunks):
        documents.append(chunk)
        metadatas.append({"url": url, "title": title, "chunk_index": idx})
        # Create a unique ID using a hash of the text so we don't overwrite existing chunks
        unique_id = hashlib.md5(chunk.encode('utf-8')).hexdigest()
        ids.append(f"custom_{unique_id}")
        
    print(f"Inserting {len(documents)} chunks into the Vector DB...")
    collection.add(
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )
    print("Successfully added to Vector DB!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python add_url_to_vector_db.py <URL>")
    else:
        url = sys.argv[1]
        add_url(url, "Custom Added URL")
