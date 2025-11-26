from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class DietPipeline:
    """Upsert dietary preferences with ingredient rules."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("diet_pipeline")

    def load_diet(self, conn, diet_id: str) -> Optional[Dict]:
        sql = """
        SELECT id, code, name, description, created_at
        FROM dietary_preferences
        WHERE id = %s;
        """
        return pg.fetch_one(conn, sql, (diet_id,))

    def load_rules(self, conn, diet_id: str) -> List[Dict]:
        sql = """
        SELECT r.id, r.ingredient_id, r.rule_type, r.notes
        FROM diet_ingredient_rules r
        WHERE r.dietary_preference_id = %s;
        """
        return pg.fetch_all(conn, sql, (diet_id,))

    def _upsert_cypher(self) -> str:
        return """
        MERGE (dp:DietaryPreference {id: $diet.id})
        SET dp.code = $diet.code,
            dp.name = $diet.name,
            dp.description = $diet.description,
            dp.created_at = datetime($diet.created_at)

        WITH dp
        OPTIONAL MATCH (dp)-[old:FORBIDS|ALLOWS|REQUIRES]->(:Ingredient)
        DELETE old;

        WITH dp, $rules AS rules
        UNWIND rules AS r
          MATCH (ing:Ingredient {id: r.ingredient_id})
          FOREACH (_ IN CASE WHEN r.rule_type = 'forbids' THEN [1] ELSE [] END |
            MERGE (dp)-[:FORBIDS]->(ing)
            SET ing.notes = coalesce(r.notes, ing.notes)
          )
          FOREACH (_ IN CASE WHEN r.rule_type = 'allows' THEN [1] ELSE [] END |
            MERGE (dp)-[:ALLOWS]->(ing)
            SET ing.notes = coalesce(r.notes, ing.notes)
          )
          FOREACH (_ IN CASE WHEN r.rule_type = 'requires' THEN [1] ELSE [] END |
            MERGE (dp)-[:REQUIRES]->(ing)
            SET ing.notes = coalesce(r.notes, ing.notes)
          );
        """

    def _delete_cypher(self) -> str:
        return "MATCH (dp:DietaryPreference {id: $id}) DETACH DELETE dp;"

    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            diet = self.load_diet(conn, event.aggregate_id)

        if diet is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting diet", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Diet missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            rules = self.load_rules(conn, event.aggregate_id)

        params = {"diet": diet, "rules": rules}
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info("Upserted diet", extra={"id": event.aggregate_id, "rules": len(rules)})
