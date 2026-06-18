import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag

def scrape_api_sql_functions():
    url = "https://spark.apache.org/docs/latest/api/sql/index.html"
    print(f"Fetching API SQL functions from {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    entries = []
    
    # In api/sql/index.html, functions are listed under <h3> tags with an ID
    for h3 in soup.find_all('h3'):
        if not h3.get('id'):
            continue
            
        name = h3.text.strip()
        func_url = url + "#" + h3['id']
        
        parameters = ""
        returns_desc = ""
        examples = ""
        
        # Iterate over siblings until the next h3 or end of section
        for sibling in h3.find_next_siblings():
            if sibling.name in ['h1', 'h2', 'h3']:
                break
                
            text = sibling.text.strip()
            
            if sibling.name == 'p':
                # Check if it contains 'Arguments:'
                if text.startswith('Arguments:') or sibling.find(text=lambda t: t and 'Arguments:' in t):
                    parameters += text + "\n"
                elif text.startswith('Examples:') or sibling.find(text=lambda t: t and 'Examples:' in t):
                    pass # Handled by pre tag usually
                else:
                    returns_desc += text + "\n\n"
            
            elif sibling.name == 'ul':
                parameters += text + "\n"
                
            elif sibling.name == 'pre':
                examples += text + "\n\n"

        entries.append({
            "name": name,
            "type": "function",
            "url": func_url,
            "parameters": parameters.strip(),
            "returns": returns_desc.strip(),
            "examples": examples.strip()
        })
        
    return entries

def scrape_sql_references():
    base_url = "https://spark.apache.org/docs/latest/sql-ref.html"
    print(f"Fetching SQL Reference index from {base_url}")
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all sub-links starting with sql-ref-
    sublinks = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        href_no_frag = urldefrag(href)[0]
        if href_no_frag.startswith("sql-ref-") and href_no_frag.endswith(".html"):
            sublinks.add(urljoin(base_url, href_no_frag))
            
    entries = []
    for link in sorted(sublinks):
        print(f"  Crawling sublink: {link}")
        res = requests.get(link)
        sub_soup = BeautifulSoup(res.text, 'html.parser')
        
        # Extract content by sections (h2 or h3)
        for heading in sub_soup.find_all(['h2', 'h3']):
            if not heading.get('id'):
                continue
                
            name = heading.text.strip()
            sec_url = link + "#" + heading['id']
            description = ""
            examples = ""
            
            for sibling in heading.find_next_siblings():
                if sibling.name in ['h1', 'h2', 'h3']:
                    break
                
                if sibling.name == 'pre' or sibling.name == 'figure':
                    examples += sibling.text.strip() + "\n\n"
                elif sibling.name in ['p', 'ul', 'ol', 'table']:
                    description += sibling.text.strip() + "\n\n"
                    
            entries.append({
                "name": name,
                "type": "reference",
                "url": sec_url,
                "parameters": "N/A",
                "returns": description.strip(),
                "examples": examples.strip()
            })
            
    return entries

def main():
    print("Starting Spark SQL Documentation Scraper...")
    os.makedirs("documents", exist_ok=True)
    
    all_entries = []
    
    # 1. Scrape built-in functions
    all_entries.extend(scrape_api_sql_functions())
    
    # 2. Scrape SQL reference guide
    all_entries.extend(scrape_sql_references())
    
    # Write to JSON
    output_path = os.path.join("documents", "spark_sql_functions.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_entries, f, indent=2, ensure_ascii=False)
        
    print(f"Successfully scraped {len(all_entries)} entries.")
    print(f"Data saved to {output_path}")

if __name__ == "__main__":
    main()
