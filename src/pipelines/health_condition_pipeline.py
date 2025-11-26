from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class HealthConditionPipeline:
    """Upsert health conditions with nutrient thresholds and ingredient restrictions."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("health_condition_pipeline")

    def load_condition(self, conn, condition_id: str) -> Optional[Dict]:
        sql = """
        SELECT id, code, name, category, description, icd_10_code, prevalence_pct, created_at
        FROM health_conditions
        WHERE id = %s;
        """
        return pg.fetch_one(conn, sql, (condition_id,))

    def load_nutrient_thresholds(self, conn, condition_id: str) -> List[Dict]:
        sql = """
        SELECT t.id, t.nutrient_id, t.min_daily_mg, t.max_daily_mg, t.target_daily_mg,
               t.severity_modifier, t.guideline_source
        FROM health_condition_nutrient_thresholds t
        WHERE t.condition_id = %s;
        """
        return pg.fetch_all(conn, sql, (condition_id,))

    def load_ingredient_restrictions(self, conn, condition_id: str) -> List[Dict]:
        sql = """
        SELECT r.id, r.ingredient_id, r.restriction_type, r.max_daily_amount_g,
               r.reasoning, r.guideline_source
        FROM health_condition_ingredient_restrictions r
        WHERE r.condition_id = %s;
        """
        return pg.fetch_all(conn, sql, (condition_id,))

    def _upsert_cypher(self) -> str:
        return """
        MERGE (hc:HealthCondition {id: $condition.id})
        SET hc.code = $condition.code,
            hc.name = $condition.name,
            hc.category = $condition.category,
            hc.description = $condition.description,
            hc.icd_10_code = $condition.icd_10_code,
            hc.prevalence_pct = $condition.prevalence_pct,
            hc.created_at = datetime($condition.created_at)

        // Clear old relationships
        WITH hc
        OPTIONAL MATCH (hc)-[oldN:HAS_NUTRIENT_THRESHOLD]->(:NutrientDefinition)
        DELETE oldN;
        OPTIONAL MATCH (hc)-[oldI:RESTRICTS_INGREDIENT]->(:Ingredient)
        DELETE oldI;

        // Nutrient thresholds
        WITH hc, $nutrient_thresholds AS nts
        UNWIND nts AS nt
          MATCH (nd:NutrientDefinition {id: nt.nutrient_id})
          MERGE (hc)-[rel:HAS_NUTRIENT_THRESHOLD]->(nd)
          SET rel.min_daily_mg = nt.min_daily_mg,
              rel.max_daily_mg = nt.max_daily_mg,
              rel.target_daily_mg = nt.target_daily_mg,
              rel.severity_modifier = nt.severity_modifier,
              rel.guideline_source = nt.guideline_source

        // Ingredient restrictions
        WITH hc, $ingredient_restrictions AS irs
        UNWIND irs AS ir
          MATCH (ing:Ingredient {id: ir.ingredient_id})
          MERGE (hc)-[rel:RESTRICTS_INGREDIENT]->(ing)
          SET rel.restriction_type = ir.restriction_type,
              rel.max_daily_amount_g = ir.max_daily_amount_g,
              rel.reasoning = ir.reasoning,
              rel.guideline_source = ir.guideline_source;
        """

    def _delete_cypher(self) -> str:
        return "MATCH (hc:HealthCondition {id: $id}) DETACH DELETE hc;"

    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            condition = self.load_condition(conn, event.aggregate_id)

        if condition is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting health condition", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Health condition missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            thresholds = self.load_nutrient_thresholds(conn, event.aggregate_id)
            restrictions = self.load_ingredient_restrictions(conn, event.aggregate_id)

        params = {
            "condition": condition,
            "nutrient_thresholds": thresholds,
            "ingredient_restrictions": restrictions,
        }
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info(
            "Upserted health condition",
            extra={"id": event.aggregate_id, "thresholds": len(thresholds), "restrictions": len(restrictions)},
        )
