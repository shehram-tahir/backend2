from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict
import anthropic
import json
from langchain_community.vectorstores import FAISS
from langchain_core.embeddings import Embeddings
from langchain.embeddings import CacheBackedEmbeddings
from langchain_anthropic import ChatAnthropic
from langchain.text_splitter import CharacterTextSplitter
from langchain_openai import OpenAIEmbeddings  # or another embeddings model of your choice


with open('secrets/secret_LLM_api_key.json') as f:
    api_key = json.load(f)['api_key']
    
client = anthropic.Client(api_key=api_key)
# Or load from environment variable:
# client = anthropic.Client(api_key=os.getenv('ANTHROPIC_API_KEY'))

class BusinessPromptRequest(BaseModel):
    user_prompt: str

class BusinessPromptResponse(BaseModel):
    is_complete: bool
    missing_information: Optional[List[str]] = None
    system_limitations: Optional[List[str]] = None
    suggested_prompt: Optional[str] = None
    can_proceed: bool
    next_steps: Optional[str] = None

# Your system's capabilities
SYSTEM_CAPABILITIES = {
    "countries": {
        "Saudi Arabia": {
            "cities": ["City1", "City2", "City3"],
            "business_types": ["restaurant", "cafe", "retail"],
            "available_data": ["demographics", "competition", "real_estate"]
        }
    }
}

# Initialize vector store for system capabilities


def create_vector_store():
    capabilities_docs = []
    for country, country_data in SYSTEM_CAPABILITIES["countries"].items():
        doc = f"""
        Country: {country}
        Available Cities: {', '.join(country_data['cities'])}
        Business Types: {', '.join(country_data['business_types'])}
        Available Data: {', '.join(country_data['available_data'])}
        """
        capabilities_docs.append(doc)
    
    text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
    docs = text_splitter.split_text('\n'.join(capabilities_docs))
    
    # Using OpenAI's embeddings model instead
    embeddings = OpenAIEmbeddings(api_key=api_key)
    
    vector_store = FAISS.from_texts(docs, embeddings)
    return vector_store

# RAG query function
async def query_capabilities(query: str, vector_store) -> str:
    # Search relevant capabilities
    relevant_docs = vector_store.similarity_search(query, k=2)
    
    # Combine relevant information
    context = "\n".join([doc.page_content for doc in relevant_docs])
    
    return context

async def analyze_prompt_completeness(prompt: str, vector_store) -> BusinessPromptResponse:
    # First, retrieve relevant system capabilities
    capabilities_context = await query_capabilities(prompt, vector_store)
    
    system_message = f"""
    You are a business location analyst assistant. Use the following information about our system capabilities:
    
    {capabilities_context}
    
    Analyze if the given prompt includes:
    1. Business type (specific, e.g., 'pizza restaurant' not just 'business')
    2. Desired location/city
    3. Any specific requirements (e.g., size, budget, target audience)
    
    Only accept cities and business types listed in our capabilities.
    
    Respond in valid JSON format with:
    {{
        "is_complete": boolean,
        "missing_information": [list of missing elements],
        "system_limitations": [any requested locations/data we don't support],
        "suggested_prompt": "complete version of their prompt if incomplete",
        "can_proceed": boolean,
        "next_steps": "what the system should do next"
    }}
    """

    try:
        response = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            messages=[{
                "role": "user",
                "content": f"{system_message}\n\nAnalyze this prompt: {prompt}"
            }]
        )
        
        analysis = json.loads(response.content[0].text)
        return BusinessPromptResponse(**analysis)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

