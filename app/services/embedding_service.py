"""
Embedding Service
Generates and manages vector embeddings for semantic search
"""
import logging
from typing import List, Dict, Optional, Any
from openai import AsyncOpenAI
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class EmbeddingService:
    """Service for generating and managing embeddings"""
    
    # OpenAI embedding model (1536 dimensions, cheap and fast)
    EMBEDDING_MODEL = "text-embedding-3-small"
    EMBEDDING_DIMENSIONS = 1536
    
    # Batch size for embedding generation
    BATCH_SIZE = 100
    
    # Table configuration: what text to embed for each table
    TABLE_CONFIGS = {
        "tickets": {
            "fields": ["ticket_number", "title", "description", "resolution"],
            "template": "Ticket #{ticket_number}: {title}\n\nDescription: {description}\n\nResolution: {resolution}",
            "fallback": "Ticket #{ticket_number}: {title}",
            "enabled": True
        },
        "ticket_notes": {
            "fields": ["title", "description"],
            "template": "Note: {title}\n\n{description}",
            "fallback": "{description}",
            "enabled": True
        },
        "resources": {
            "fields": ["first_name", "last_name", "title", "user_name"],
            "template": "{first_name} {last_name} - {title} ({user_name})",
            "fallback": "{first_name} {last_name}",
            "enabled": True
        },
        "contacts": {
            "fields": ["first_name", "last_name", "title", "email_address", "note"],
            "template": "{first_name} {last_name} - {title}\nEmail: {email_address}\nNote: {note}",
            "fallback": "{first_name} {last_name}",
            "enabled": True
        },
        "companies": {
            "fields": ["company_name", "city", "state", "web_address", "additional_address_information"],
            "template": "{company_name}\nLocation: {city}, {state}\nWebsite: {web_address}\nNotes: {additional_address_information}",
            "fallback": "{company_name}",
            "enabled": True
        },
        "time_entries": {
            "fields": ["summary_notes", "internal_notes"],
            "template": "Summary: {summary_notes}\n\nInternal: {internal_notes}",
            "fallback": "{summary_notes}",
            "enabled": False  # Disable by default (lots of entries, might not need search)
        }
    }
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.openai_api_key)
    
    async def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text
        
        Args:
            text: Text to embed
            
        Returns:
            List of floats (vector)
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for embedding")
            return [0.0] * self.EMBEDDING_DIMENSIONS
        
        try:
            # Truncate if too long (max 8191 tokens for text-embedding-3-small)
            text = text[:32000]  # Rough approximation
            
            response = await self.client.embeddings.create(
                model=self.EMBEDDING_MODEL,
                input=text,
                dimensions=self.EMBEDDING_DIMENSIONS
            )
            
            return response.data[0].embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
    
    async def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in one API call
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings
        """
        if not texts:
            return []
        
        # Filter out empty texts but keep track of indices
        valid_texts = []
        valid_indices = []
        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text[:32000])
                valid_indices.append(i)
        
        if not valid_texts:
            logger.warning("No valid texts to embed")
            return [[0.0] * self.EMBEDDING_DIMENSIONS] * len(texts)
        
        try:
            response = await self.client.embeddings.create(
                model=self.EMBEDDING_MODEL,
                input=valid_texts,
                dimensions=self.EMBEDDING_DIMENSIONS
            )
            
            # Create result list with embeddings in correct positions
            embeddings = [[0.0] * self.EMBEDDING_DIMENSIONS] * len(texts)
            for i, embedding_data in enumerate(response.data):
                original_index = valid_indices[i]
                embeddings[original_index] = embedding_data.embedding
            
            return embeddings
            
        except Exception as e:
            logger.error(f"Error generating batch embeddings: {e}")
            raise
    
    def prepare_text_for_embedding(self, table: str, row: Dict) -> str:
        """
        Prepare text from a database row for embedding
        
        Args:
            table: Table name
            row: Database row as dictionary
            
        Returns:
            Formatted text ready for embedding
        """
        config = self.TABLE_CONFIGS.get(table)
        if not config:
            logger.warning(f"No config for table: {table}")
            return ""
        
        try:
            # Try to use template
            text = config["template"].format(**{
                k: str(row.get(k, "")).strip() 
                for k in config["fields"]
            })
            
            # If template produces mostly empty string, use fallback
            if len(text.strip()) < 10 and config.get("fallback"):
                text = config["fallback"].format(**{
                    k: str(row.get(k, "")).strip() 
                    for k in config["fields"]
                })
            
            # Clean up the text
            text = text.replace("\n\n\n", "\n\n")  # Remove excessive newlines
            text = text.replace("None", "")  # Remove "None" strings
            text = " ".join(text.split())  # Normalize whitespace
            
            return text.strip()
            
        except Exception as e:
            logger.error(f"Error preparing text for {table}: {e}")
            # Return fallback
            if config.get("fallback"):
                try:
                    return config["fallback"].format(**{
                        k: str(row.get(k, "")).strip() 
                        for k in config["fields"]
                    })
                except:
                    pass
            return ""
    
    async def search_similar(
        self, 
        table: str,
        query_text: str,
        limit: int = 10,
        threshold: float = 0.7
    ) -> List[Dict]:
        """
        Search for similar items using vector similarity
        
        Args:
            table: Table to search
            query_text: Search query
            limit: Number of results
            threshold: Similarity threshold (0-1)
            
        Returns:
            List of matching rows with similarity scores
        """
        # Generate embedding for query
        query_embedding = await self.generate_embedding(query_text)
        
        # Note: This would use the database service to perform vector search
        # Implementation will be in the hybrid AI service
        pass
    
    def get_enabled_tables(self) -> List[str]:
        """Get list of tables that have embedding enabled"""
        return [
            table 
            for table, config in self.TABLE_CONFIGS.items() 
            if config.get("enabled", False)
        ]
    
    def is_table_enabled(self, table: str) -> bool:
        """Check if embedding is enabled for a table"""
        config = self.TABLE_CONFIGS.get(table, {})
        return config.get("enabled", False)
    
    async def estimate_cost(self, total_records: int) -> Dict[str, float]:
        """
        Estimate embedding cost
        
        Args:
            total_records: Number of records to embed
            
        Returns:
            Cost estimates
        """
        # text-embedding-3-small pricing: $0.02 per 1M tokens
        # Rough estimate: 500 tokens per record on average
        estimated_tokens = total_records * 500
        cost_per_million = 0.02
        
        estimated_cost = (estimated_tokens / 1_000_000) * cost_per_million
        
        return {
            "total_records": total_records,
            "estimated_tokens": estimated_tokens,
            "estimated_cost_usd": round(estimated_cost, 2),
            "model": self.EMBEDDING_MODEL
        }


def get_embedding_service() -> EmbeddingService:
    """Get embedding service instance"""
    return EmbeddingService()