"""
Embedding Sync Script
Embeds all existing records in the database

Usage:
    python -m app.scripts.embed_all_records

Options:
    --table: Specific table to embed (default: all)
    --batch-size: Batch size for processing (default: 100)
    --force: Re-embed even if already embedded
    --dry-run: Show what would be done without doing it
"""
import asyncio
import sys
import argparse
from datetime import datetime
from typing import List, Dict
import logging

# Add parent directory to path
sys.path.insert(0, '.')

from app.services.database import get_database_service
from app.services.embedding_service import get_embedding_service

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmbeddingSyncManager:
    """Manages the embedding sync process"""
    
    def __init__(self, batch_size: int = 100, force: bool = False, dry_run: bool = False):
        self.db = get_database_service()
        self.embedding_service = get_embedding_service()
        self.batch_size = batch_size
        self.force = force
        self.dry_run = dry_run
        
        self.stats = {
            "total_processed": 0,
            "total_embedded": 0,
            "total_skipped": 0,
            "total_errors": 0,
            "by_table": {}
        }
    
    async def sync_table(self, table: str) -> Dict:
        """
        Sync embeddings for a specific table
        
        Args:
            table: Table name
            
        Returns:
            Stats dictionary
        """
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting sync for table: {table}")
        
        if not self.embedding_service.is_table_enabled(table):
            logger.warning(f"Table {table} is not enabled for embedding")
            return {"processed": 0, "embedded": 0, "skipped": 0, "errors": 0}
        
        stats = {"processed": 0, "embedded": 0, "skipped": 0, "errors": 0}
        
        # Get total count
        count_query = self.db.client.table(table).select("id", count="exact")
        if not self.force:
            # Only get records without embeddings
            count_query = count_query.is_("embedding", "null")
        
        total_result = count_query.execute()
        total_records = total_result.count or 0
        
        logger.info(f"Found {total_records:,} records to process in {table}")
        
        if total_records == 0:
            logger.info(f"No records to process in {table}")
            return stats
        
        # Estimate cost
        cost_info = await self.embedding_service.estimate_cost(total_records)
        logger.info(f"Estimated cost: ${cost_info['estimated_cost_usd']} "
                   f"({cost_info['estimated_tokens']:,} tokens)")
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would process {total_records:,} records")
            return {"processed": total_records, "embedded": 0, "skipped": 0, "errors": 0}
        
        # Process in batches
        offset = 0
        while offset < total_records:
            logger.info(f"Processing batch {offset // self.batch_size + 1} "
                       f"({offset:,} - {min(offset + self.batch_size, total_records):,} "
                       f"of {total_records:,})")
            
            try:
                # Fetch batch
                query = self.db.client.table(table).select("*")
                if not self.force:
                    query = query.is_("embedding", "null")
                
                batch_result = query.range(offset, offset + self.batch_size - 1).execute()
                records = batch_result.data
                
                if not records:
                    break
                
                # Prepare texts
                texts = []
                valid_records = []
                for record in records:
                    text = self.embedding_service.prepare_text_for_embedding(table, record)
                    if text and len(text.strip()) > 5:  # Only embed if has meaningful content
                        texts.append(text)
                        valid_records.append(record)
                    else:
                        stats["skipped"] += 1
                        logger.debug(f"Skipping record {record.get('id')} - insufficient content")
                
                if not texts:
                    logger.warning(f"No valid texts in batch at offset {offset}")
                    offset += self.batch_size
                    continue
                
                # Generate embeddings
                logger.info(f"Generating {len(texts)} embeddings...")
                embeddings = await self.embedding_service.generate_embeddings_batch(texts)
                
                # Update records
                logger.info(f"Updating {len(valid_records)} records...")
                for record, embedding, text in zip(valid_records, embeddings, texts):
                    try:
                        update_data = {
                            "embedding": embedding,
                            "embedding_text": text[:1000],  # Store first 1000 chars for debugging
                            "last_embedded_at": datetime.utcnow().isoformat()
                        }
                        
                        self.db.client.table(table)\
                            .update(update_data)\
                            .eq("id", record["id"])\
                            .execute()
                        
                        stats["embedded"] += 1
                        
                    except Exception as e:
                        logger.error(f"Error updating record {record.get('id')}: {e}")
                        stats["errors"] += 1
                
                stats["processed"] += len(records)
                
                # Progress update
                progress_pct = (offset + len(records)) / total_records * 100
                logger.info(f"Progress: {progress_pct:.1f}% ({stats['embedded']} embedded, "
                           f"{stats['skipped']} skipped, {stats['errors']} errors)")
                
                offset += self.batch_size
                
                # Small delay to avoid rate limits
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing batch at offset {offset}: {e}")
                stats["errors"] += self.batch_size
                offset += self.batch_size
                continue
        
        logger.info(f"Completed {table}: {stats['embedded']} embedded, "
                   f"{stats['skipped']} skipped, {stats['errors']} errors")
        
        return stats
    
    async def sync_all_tables(self, specific_table: str = None) -> Dict:
        """
        Sync embeddings for all enabled tables
        
        Args:
            specific_table: If provided, only sync this table
            
        Returns:
            Overall stats
        """
        tables = [specific_table] if specific_table else self.embedding_service.get_enabled_tables()
        
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting embedding sync for tables: {tables}")
        logger.info(f"Batch size: {self.batch_size}, Force: {self.force}")
        
        for table in tables:
            try:
                table_stats = await self.sync_table(table)
                self.stats["by_table"][table] = table_stats
                self.stats["total_processed"] += table_stats["processed"]
                self.stats["total_embedded"] += table_stats["embedded"]
                self.stats["total_skipped"] += table_stats["skipped"]
                self.stats["total_errors"] += table_stats["errors"]
            except Exception as e:
                logger.error(f"Error syncing table {table}: {e}")
                self.stats["by_table"][table] = {"error": str(e)}
        
        return self.stats
    
    def print_summary(self):
        """Print summary of sync operation"""
        logger.info("\n" + "="*60)
        logger.info("EMBEDDING SYNC SUMMARY")
        logger.info("="*60)
        logger.info(f"Total processed: {self.stats['total_processed']:,}")
        logger.info(f"Total embedded: {self.stats['total_embedded']:,}")
        logger.info(f"Total skipped: {self.stats['total_skipped']:,}")
        logger.info(f"Total errors: {self.stats['total_errors']:,}")
        logger.info("\nBy Table:")
        for table, stats in self.stats["by_table"].items():
            if "error" in stats:
                logger.info(f"  {table}: ERROR - {stats['error']}")
            else:
                logger.info(f"  {table}: {stats['embedded']} embedded, "
                           f"{stats['skipped']} skipped, {stats['errors']} errors")
        logger.info("="*60)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Embed all database records")
    parser.add_argument("--table", type=str, help="Specific table to embed")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size")
    parser.add_argument("--force", action="store_true", help="Re-embed existing records")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    
    args = parser.parse_args()
    
    manager = EmbeddingSyncManager(
        batch_size=args.batch_size,
        force=args.force,
        dry_run=args.dry_run
    )
    
    try:
        await manager.sync_all_tables(specific_table=args.table)
        manager.print_summary()
    except KeyboardInterrupt:
        logger.info("\nSync interrupted by user")
        manager.print_summary()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        manager.print_summary()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())