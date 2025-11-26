import time
from typing import List

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.queue.outbox import fetch_pending_events, mark_failed, mark_processed
from src.adapters.supabase.db import PostgresPool
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.pipelines.age_pipeline import AgePipeline
from src.pipelines.allergen_pipeline import AllergenPipeline
from src.pipelines.diet_pipeline import DietPipeline
from src.pipelines.health_condition_pipeline import HealthConditionPipeline
from src.utils.logging import configure_logging


TABLES = [
    "allergens",
    "allergen_synonyms",
    "health_conditions",
    "health_condition_nutrient_thresholds",
    "health_condition_ingredient_restrictions",
    "dietary_preferences",
    "diet_ingredient_rules",
    "age_bands",
]

AGG_TYPES = ["allergen", "health_condition", "dietary_preference", "age_band"]


def process_batch(
    allergen_pipeline: AllergenPipeline,
    condition_pipeline: HealthConditionPipeline,
    diet_pipeline: DietPipeline,
    age_pipeline: AgePipeline,
    events: List[OutboxEvent],
    pg_pool: PostgresPool,
    log,
):
    for event in events:
        try:
            agg = event.aggregate_type
            if agg == "allergen":
                allergen_pipeline.handle_event(event)
            elif agg == "health_condition":
                condition_pipeline.handle_event(event)
            elif agg == "dietary_preference":
                diet_pipeline.handle_event(event)
            elif agg == "age_band":
                age_pipeline.handle_event(event)
            else:
                log.warning("Unhandled aggregate type", extra={"aggregate_type": agg, "event_id": event.id})
                continue

            with pg_pool.connection() as conn:
                mark_processed(conn, event.id)
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed processing health-safety event", extra={"event_id": event.id, "aggregate_id": event.aggregate_id})
            with pg_pool.connection() as conn:
                mark_failed(conn, event.id, str(exc))


def main():
    settings = Settings()
    log = configure_logging("health_safety_worker")
    log.info("Starting health-safety worker", extra={"pipeline": settings.pipeline_name})

    pg_pool = PostgresPool(settings.supabase_dsn)
    neo4j = Neo4jClient(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)

    allergen_pipeline = AllergenPipeline(settings, pg_pool, neo4j)
    condition_pipeline = HealthConditionPipeline(settings, pg_pool, neo4j)
    diet_pipeline = DietPipeline(settings, pg_pool, neo4j)
    age_pipeline = AgePipeline(settings, pg_pool, neo4j)

    try:
        while True:
            with pg_pool.connection() as conn:
                conn.autocommit = False
                events = fetch_pending_events(
                    conn,
                    settings.batch_size,
                    settings.max_attempts,
                    table_names=TABLES,
                    aggregate_types=AGG_TYPES,
                )
                conn.commit()

            if not events:
                time.sleep(settings.poll_interval_seconds)
                continue

            process_batch(allergen_pipeline, condition_pipeline, diet_pipeline, age_pipeline, events, pg_pool, log)
    finally:
        neo4j.close()
        pg_pool.close()


if __name__ == "__main__":
    main()
