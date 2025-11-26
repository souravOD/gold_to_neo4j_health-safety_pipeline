# Pipeline: health-safety

Scope: Ingest health & safety taxonomy/rules (allergens, conditions, diets, age bands) from Supabase Gold v3 into Neo4j v3 via a Python worker. This repo is self-contained (no shared code).

Supabase source tables: allergens, allergen_synonyms, health_conditions, health_condition_nutrient_thresholds, health_condition_ingredient_restrictions, dietary_preferences, diet_ingredient_rules, age_bands
Neo4j labels touched: Allergen, HealthCondition, DietaryPreference, NutrientDefinition, Ingredient, AgeBand
Neo4j relationships touched: HAS_NUTRIENT_THRESHOLD, RESTRICTS_INGREDIENT, FORBIDS/ALLOWS/REQUIRES (diet rules), CROSS_REACTIVE_WITH (future), AGE bands (product age edges stay in Product pipeline)

How it works
- Outbox-driven: worker polls `outbox_events` filtered to health-safety tables/aggregate types (allergen, health_condition, dietary_preference, age_band), locks with `SKIP LOCKED`, routes per aggregate.
- Allergen upsert: reloads allergen + synonyms; upserts Allergen node and synonyms property; DELETE events detach-delete missing allergens.
- Health condition upsert: reloads condition + nutrient thresholds + ingredient restrictions; rebuilds HAS_NUTRIENT_THRESHOLD and RESTRICTS_INGREDIENT edges; DELETE events detach-delete missing conditions.
- Diet upsert: reloads dietary preference + ingredient rules; rebuilds FORBIDS/ALLOWS/REQUIRES edges; DELETE events detach-delete missing diets.
- Age band upsert: reloads age_band; upserts AgeBand node; DELETE events detach-delete missing age bands. Product → age edges remain in the Product pipeline.

Run
- Install deps: `pip install -r requirements.txt`
- Configure env: copy `.env.example` → `.env` and fill Postgres/Neo4j credentials.
- Start worker: `python -m src.workers.runner`

Folders
- docs/: domain notes, Cypher patterns, event routing
- src/: config, adapters (supabase, neo4j, queue), domain models/services, pipelines (aggregate upserts), workers (runners), utils
- tests/: placeholder for unit/integration tests
- ops/: ops templates (docker/env/sample cron jobs)
