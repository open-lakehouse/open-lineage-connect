# Delta Lake Lineage Demo

## Prerequisites

- Docker
- Docker Compose

## Deployment

Start all services:

```bash
docker compose -f docker-compose-delta-graph.yaml up -d
```

Example output:
```bash
[+] up 4/4
 ✔ Network puppy-deltalake Created                 0.1s
 ✔ Container puppygraph    Started                 0.5s
 ✔ Container spark         Started                 0.4s
 ✔ Container unitycatalog  Started                 0.5s
```

## Data Preparation

Run the Spark job to parse and flatten the raw lineage events into graph tables:

```bash
docker exec spark /opt/spark/bin/spark-submit /process_lineage_events.py
```

This reads from `deltalake/lineage-events/event_kind=run` and writes three Delta tables to `deltalake/flattened_lineage_tables/`:

| Table | Role | ID Column |
|-------|------|-----------|
| `jobs` | Job vertices | `run_id` |
| `job_inputs` | CONSUMED_BY edges | — |
| `job_outputs` | PRODUCES edges | — |

## Register Tables with Unity Catalog

Register all Delta tables with the OSS Unity Catalog:

```bash
./register.sh
```

This creates the `demo` catalog, `results` schema, and registers the following external Delta tables:
- `demo.results.jobs`
- `demo.results.job_inputs`
- `demo.results.job_outputs`

## Modeling the Graph

1. Log into the PuppyGraph Web UI at http://localhost:8081 with the following credentials:
   - Username: `puppygraph`
   - Password: `puppygraph123`

2. Upload the graph schema defining:
   - **Job** vertex from `jobs`, ID: `run_id`
   - **Dataset** vertex as a union node from `job_inputs` and `job_outputs`, ID: `dataset_name`
   - **CONSUMED_BY** edge from `job_inputs` (Dataset → Job)
   - **PRODUCES** edge from `job_outputs` (Job → Dataset)

## Querying the Graph

Navigate to the Query panel and use the Cypher console.

### 1. Trace inputs and outputs for a specific job

```cypher
MATCH path = (input:Dataset)-[:CONSUMED_BY]->(j:Job)-[:PRODUCES]->(output:Dataset)
WHERE j.job_name = 'command'
RETURN path
```

### 2. Find multi-hop lineage chains

Traverse any number of intermediate jobs between a source and destination dataset:

```cypher
MATCH path = (d:Dataset)-[:CONSUMED_BY|PRODUCES*2..6]->(output:Dataset)
RETURN nodes(path), path
```

### 3. Find pipelines with intermediate jobs

Find cases where the output of one job becomes the input of another:

```cypher
MATCH path = (d:Dataset)-[:CONSUMED_BY]->(j1:Job)-[:PRODUCES]->(mid:Dataset)-[:CONSUMED_BY]->(j2:Job)-[:PRODUCES]->(output:Dataset)
RETURN d, j1, mid, j2, output
```

### 4. Which jobs have no outputs?

```cypher
MATCH (j:Job)
WHERE NOT (j)-[:PRODUCES]->(:Dataset)
RETURN j.job_name, j.job_namespace
```

## Cleanup and Teardown

To stop and remove all containers and networks:

```bash
docker compose -f docker-compose-delta-graph.yaml down --remove-orphans
```