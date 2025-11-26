from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class AgePipeline:
    """Upsert age bands. Product age restrictions remain in the Product pipeline."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("age_pipeline")

    def load_age_band(self, conn, age_band_id: str) -> Optional[Dict]:
        sql = """
        SELECT id, code, category, min_age_months, max_age_months, description, dietary_guidelines, created_at
        FROM age_bands
        WHERE id = %s;
        """
        return pg.fetch_one(conn, sql, (age_band_id,))

    def _upsert_cypher(self) -> str:
        return """
        MERGE (ab:AgeBand {id: $age_band.id})
        SET ab.code = $age_band.code,
            ab.category = $age_band.category,
            ab.min_age_months = $age_band.min_age_months,
            ab.max_age_months = $age_band.max_age_months,
            ab.description = $age_band.description,
            ab.dietary_guidelines = $age_band.dietary_guidelines,
            ab.created_at = datetime($age_band.created_at)
        """

    def _delete_cypher(self) -> str:
        return "MATCH (ab:AgeBand {id: $id}) DETACH DELETE ab;"

    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            age_band = self.load_age_band(conn, event.aggregate_id)

        if age_band is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting age band", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Age band missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        params = {"age_band": age_band}
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info("Upserted age band", extra={"id": event.aggregate_id})
