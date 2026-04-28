package ingest

import (
	"encoding/json"
	"testing"
)

func TestConvertEvent_RunEvent(t *testing.T) {
	input := `{
		"eventTime": "2024-06-15T10:30:00Z",
		"producer": "https://example.com/producer",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"eventType": "START",
		"run": {
			"runId": "d4c1e6a2-7b3f-4e5a-9c8d-1f2e3a4b5c6d",
			"facets": {
				"nominalTime": {
					"_producer": "https://example.com/producer",
					"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json"
				}
			}
		},
		"job": {
			"namespace": "spark-cluster",
			"name": "etl_pipeline.transform"
		},
		"inputs": [
			{
				"namespace": "postgres://db:5432",
				"name": "public.users",
				"facets": {},
				"inputFacets": {
					"dataQuality": {
						"_producer": "https://example.com/producer",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityFacet.json"
					}
				}
			}
		],
		"outputs": [
			{
				"namespace": "s3://bucket",
				"name": "curated/users",
				"facets": {},
				"outputFacets": {
					"rowCount": {
						"_producer": "https://example.com/producer",
						"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OutputStatistics.json"
					}
				}
			}
		]
	}`

	evt, err := ConvertEvent([]byte(input))
	if err != nil {
		t.Fatalf("ConvertEvent failed: %v", err)
	}

	re := evt.GetRunEvent()
	if re == nil {
		t.Fatal("expected RunEvent, got nil")
	}
	if re.EventType != "START" {
		t.Errorf("EventType = %q, want START", re.EventType)
	}
	if re.Producer != "https://example.com/producer" {
		t.Errorf("Producer = %q", re.Producer)
	}
	if re.SchemaUrl != "https://openlineage.io/spec/2-0-2/OpenLineage.json" {
		t.Errorf("SchemaUrl = %q", re.SchemaUrl)
	}
	if re.Run == nil || re.Run.RunId != "d4c1e6a2-7b3f-4e5a-9c8d-1f2e3a4b5c6d" {
		t.Errorf("Run.RunId = %q", re.GetRun().GetRunId())
	}
	if re.Run.Facets == nil {
		t.Error("Run.Facets is nil, expected nominalTime facet")
	}
	if re.Job == nil || re.Job.Namespace != "spark-cluster" || re.Job.Name != "etl_pipeline.transform" {
		t.Errorf("Job = %+v", re.Job)
	}
	if len(re.Inputs) != 1 {
		t.Fatalf("len(Inputs) = %d, want 1", len(re.Inputs))
	}
	if re.Inputs[0].Namespace != "postgres://db:5432" {
		t.Errorf("Inputs[0].Namespace = %q", re.Inputs[0].Namespace)
	}
	if re.Inputs[0].InputFacets == nil {
		t.Error("Inputs[0].InputFacets is nil")
	}
	if len(re.Outputs) != 1 {
		t.Fatalf("len(Outputs) = %d, want 1", len(re.Outputs))
	}
	if re.Outputs[0].OutputFacets == nil {
		t.Error("Outputs[0].OutputFacets is nil")
	}
	if re.RawJson == "" {
		t.Error("RawJson is empty")
	}
}

func TestConvertEvent_JobEvent(t *testing.T) {
	input := `{
		"eventTime": "2024-06-15T12:00:00Z",
		"producer": "https://example.com/producer",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"job": {
			"namespace": "airflow-prod",
			"name": "dag.task",
			"facets": {
				"sql": {
					"_producer": "https://example.com/producer",
					"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
					"query": "SELECT 1"
				}
			}
		},
		"inputs": [
			{"namespace": "bigquery", "name": "project.dataset.table", "facets": {}, "inputFacets": {}}
		],
		"outputs": []
	}`

	evt, err := ConvertEvent([]byte(input))
	if err != nil {
		t.Fatalf("ConvertEvent failed: %v", err)
	}

	je := evt.GetJobEvent()
	if je == nil {
		t.Fatal("expected JobEvent, got nil")
	}
	if je.Producer != "https://example.com/producer" {
		t.Errorf("Producer = %q", je.Producer)
	}
	if je.Job == nil || je.Job.Namespace != "airflow-prod" || je.Job.Name != "dag.task" {
		t.Errorf("Job = %+v", je.Job)
	}
	if je.Job.Facets == nil {
		t.Error("Job.Facets is nil")
	}
	if len(je.Inputs) != 1 {
		t.Errorf("len(Inputs) = %d, want 1", len(je.Inputs))
	}
	if je.RawJson == "" {
		t.Error("RawJson is empty")
	}
}

func TestConvertEvent_DatasetEvent(t *testing.T) {
	input := `{
		"eventTime": "2024-06-15T08:00:00Z",
		"producer": "https://example.com/producer",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"dataset": {
			"namespace": "s3://data-lake",
			"name": "raw/clickstream/2024-06-15",
			"facets": {
				"schema": {
					"_producer": "https://example.com/producer",
					"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
					"fields": [{"name": "id", "type": "STRING"}]
				}
			}
		}
	}`

	evt, err := ConvertEvent([]byte(input))
	if err != nil {
		t.Fatalf("ConvertEvent failed: %v", err)
	}

	de := evt.GetDatasetEvent()
	if de == nil {
		t.Fatal("expected DatasetEvent, got nil")
	}
	if de.Producer != "https://example.com/producer" {
		t.Errorf("Producer = %q", de.Producer)
	}
	if de.Dataset == nil || de.Dataset.Namespace != "s3://data-lake" {
		t.Errorf("Dataset = %+v", de.Dataset)
	}
	if de.Dataset.Facets == nil {
		t.Error("Dataset.Facets is nil")
	}
	if de.RawJson == "" {
		t.Error("RawJson is empty")
	}
}

func TestConvertEvent_InvalidJSON(t *testing.T) {
	_, err := ConvertEvent([]byte(`{not valid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestConvertEvent_MissingEventTime(t *testing.T) {
	input := `{
		"producer": "test",
		"schemaURL": "test",
		"run": {"runId": "abc"},
		"job": {"namespace": "ns", "name": "n"}
	}`
	_, err := ConvertEvent([]byte(input))
	if err == nil {
		t.Fatal("expected error for missing eventTime")
	}
}

func TestConvertEvent_UnclassifiableEvent(t *testing.T) {
	input := `{
		"eventTime": "2024-06-15T08:00:00Z",
		"producer": "test",
		"schemaURL": "test"
	}`
	_, err := ConvertEvent([]byte(input))
	if err == nil {
		t.Fatal("expected error for unclassifiable event")
	}
}

func TestConvertEvent_BadEventTime(t *testing.T) {
	input := `{
		"eventTime": "not-a-timestamp",
		"producer": "test",
		"schemaURL": "test",
		"run": {"runId": "abc"},
		"job": {"namespace": "ns", "name": "n"}
	}`
	_, err := ConvertEvent([]byte(input))
	if err == nil {
		t.Fatal("expected error for bad eventTime format")
	}
}

func TestConvertBatch_MixedEvents(t *testing.T) {
	input := `[
		{
			"eventTime": "2024-06-15T10:30:00Z",
			"producer": "p1",
			"schemaURL": "s1",
			"eventType": "COMPLETE",
			"run": {"runId": "r1"},
			"job": {"namespace": "ns1", "name": "job1"}
		},
		{
			"eventTime": "2024-06-15T11:00:00Z",
			"producer": "p2",
			"schemaURL": "s2",
			"job": {"namespace": "ns2", "name": "job2"}
		},
		{
			"eventTime": "2024-06-15T08:00:00Z",
			"producer": "p3",
			"schemaURL": "s3",
			"dataset": {"namespace": "ns3", "name": "ds1"}
		}
	]`

	events, err := ConvertBatch([]byte(input))
	if err != nil {
		t.Fatalf("ConvertBatch failed: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3", len(events))
	}

	if events[0].GetRunEvent() == nil {
		t.Error("events[0] should be RunEvent")
	}
	if events[1].GetJobEvent() == nil {
		t.Error("events[1] should be JobEvent")
	}
	if events[2].GetDatasetEvent() == nil {
		t.Error("events[2] should be DatasetEvent")
	}
}

func TestConvertBatch_InvalidArray(t *testing.T) {
	_, err := ConvertBatch([]byte(`not an array`))
	if err == nil {
		t.Fatal("expected error for non-array input")
	}
}

func TestConvertBatch_EmptyArray(t *testing.T) {
	events, err := ConvertBatch([]byte(`[]`))
	if err != nil {
		t.Fatalf("ConvertBatch failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("len(events) = %d, want 0", len(events))
	}
}

func TestConvertBatch_ErrorInOneEvent(t *testing.T) {
	input := `[
		{
			"eventTime": "2024-06-15T10:30:00Z",
			"producer": "p1",
			"schemaURL": "s1",
			"run": {"runId": "r1"},
			"job": {"namespace": "ns1", "name": "job1"}
		},
		{
			"producer": "p2",
			"schemaURL": "s2",
			"run": {"runId": "r2"},
			"job": {"namespace": "ns2", "name": "job2"}
		}
	]`

	_, err := ConvertBatch([]byte(input))
	if err == nil {
		t.Fatal("expected error when one event in batch is invalid")
	}
}

func TestClassifyEvent(t *testing.T) {
	tests := []struct {
		name string
		raw  rawEvent
		want eventKind
	}{
		{
			name: "run+job -> RunEvent",
			raw:  rawEvent{Run: ptrRawMsg(`{}`), Job: ptrRawMsg(`{}`)},
			want: eventTypeRun,
		},
		{
			name: "job only -> JobEvent",
			raw:  rawEvent{Job: ptrRawMsg(`{}`)},
			want: eventTypeJob,
		},
		{
			name: "dataset only -> DatasetEvent",
			raw:  rawEvent{Dataset: ptrRawMsg(`{}`)},
			want: eventTypeDataset,
		},
		{
			name: "nothing -> Unknown",
			raw:  rawEvent{},
			want: eventTypeUnknown,
		},
		{
			name: "run only (no job) -> Unknown",
			raw:  rawEvent{Run: ptrRawMsg(`{}`)},
			want: eventTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyEvent(&tt.raw)
			if got != tt.want {
				t.Errorf("classifyEvent() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestConvertEvent_FacetsWithUnderscoreKeys(t *testing.T) {
	input := `{
		"eventTime": "2024-06-15T10:30:00Z",
		"producer": "test",
		"schemaURL": "test",
		"eventType": "START",
		"run": {
			"runId": "abc",
			"facets": {
				"myFacet": {
					"_producer": "test",
					"_schemaURL": "test-schema",
					"_deleted": true,
					"customField": "value"
				}
			}
		},
		"job": {"namespace": "ns", "name": "j"}
	}`

	evt, err := ConvertEvent([]byte(input))
	if err != nil {
		t.Fatalf("ConvertEvent failed: %v", err)
	}

	re := evt.GetRunEvent()
	if re == nil || re.Run == nil || re.Run.Facets == nil {
		t.Fatal("expected RunEvent with facets")
	}
	facetMap := re.Run.Facets.AsMap()
	myFacet, ok := facetMap["myFacet"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected myFacet to be a map, got %T", facetMap["myFacet"])
	}
	if myFacet["_producer"] != "test" {
		t.Errorf("_producer = %v", myFacet["_producer"])
	}
	if myFacet["_deleted"] != true {
		t.Errorf("_deleted = %v", myFacet["_deleted"])
	}
}

func TestConvertEvent_MinimalRunEvent(t *testing.T) {
	input := `{
		"eventTime": "2024-01-01T00:00:00Z",
		"producer": "p",
		"schemaURL": "s",
		"eventType": "OTHER",
		"run": {"runId": "r"},
		"job": {"namespace": "ns", "name": "n"}
	}`

	evt, err := ConvertEvent([]byte(input))
	if err != nil {
		t.Fatalf("ConvertEvent failed: %v", err)
	}
	re := evt.GetRunEvent()
	if re == nil {
		t.Fatal("expected RunEvent")
	}
	if re.Run.Facets != nil {
		t.Error("expected nil facets for minimal run")
	}
	if len(re.Inputs) != 0 {
		t.Error("expected no inputs for minimal run")
	}
	if len(re.Outputs) != 0 {
		t.Error("expected no outputs for minimal run")
	}
}

func ptrRawMsg(s string) *json.RawMessage {
	raw := json.RawMessage(s)
	return &raw
}
