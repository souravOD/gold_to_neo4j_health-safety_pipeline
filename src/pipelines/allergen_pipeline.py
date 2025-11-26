from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class AllergenPipeline:
    """Upsert allergen taxonomy (allergens + synonyms)."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("allergen_pipeline")

    def load_allergen(self, conn, allergen_id: str) -> Optional[Dict]:
        sql = """
        SELECT id, code, name, common_names, category, is_top_9, severity_typical,
               description, cross_reactive_with, regulatory_region, created_at
        FROM allergens
        WHERE id = %s;
        """
        return pg.fetch_one(conn, sql, (allergen_id,))

    def load_synonyms(self, conn, allergen_id: str) -> List[str]:
        sql = """
        SELECT synonym
        FROM allergen_synonyms
        WHERE canonical_allergen_id = %s;
        """
        rows = pg.fetch_all(conn, sql, (allergen_id,))
        return [r["synonym"] for r in rows] if rows else []

    def _upsert_cypher(self) -> str:
        return """
        MERGE (a:Allergen {id: $allergen.id})
        SET a.code = $allergen.code,
            a.name = $allergen.name,
            a.common_names = $allergen.common_names,
            a.category = $allergen.category,
            a.is_top_9 = $allergen.is_top_9,
            a.severity_typical = $allergen.severity_typical,
            a.description = $allergen.description,
            a.cross_reactive_with = $allergen.cross_reactive_with,
            a.regulatory_region = $allergen.regulatory_region,
            a.synonyms = $synonyms,
            a.created_at = datetime($allergen.created_at)
        """

    def _delete_cypher(self) -> str:
        return "MATCH (a:Allergen {id: $id}) DETACH DELETE a;"

    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            allergen = self.load_allergen(conn, event.aggregate_id)

        if allergen is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting allergen", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Allergen missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            synonyms = self.load_synonyms(conn, event.aggregate_id)

        params = {"allergen": allergen, "synonyms": synonyms}
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info("Upserted allergen", extra={"id": event.aggregate_id, "synonyms": len(synonyms)})
