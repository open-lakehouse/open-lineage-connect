package ingest

import (
	"encoding/json"
	"fmt"
	"time"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// rawEvent is the intermediate JSON representation used for event-type
// discrimination. We unmarshal into this struct to inspect which top-level
// fields are present before converting to the appropriate proto message.
type rawEvent struct {
	EventTime string           `json:"eventTime"`
	Producer  string           `json:"producer"`
	SchemaURL string           `json:"schemaURL"`
	EventType string           `json:"eventType,omitempty"`
	Run       *json.RawMessage `json:"run,omitempty"`
	Job       *json.RawMessage `json:"job,omitempty"`
	Dataset   *json.RawMessage `json:"dataset,omitempty"`
	Inputs    []json.RawMessage `json:"inputs,omitempty"`
	Outputs   []json.RawMessage `json:"outputs,omitempty"`
}

type rawRun struct {
	RunID  string                 `json:"runId"`
	Facets map[string]interface{} `json:"facets,omitempty"`
}

type rawJob struct {
	Namespace string                 `json:"namespace"`
	Name      string                 `json:"name"`
	Facets    map[string]interface{} `json:"facets,omitempty"`
}

type rawDataset struct {
	Namespace    string                 `json:"namespace"`
	Name         string                 `json:"name"`
	Facets       map[string]interface{} `json:"facets,omitempty"`
	InputFacets  map[string]interface{} `json:"inputFacets,omitempty"`
	OutputFacets map[string]interface{} `json:"outputFacets,omitempty"`
}

// ConvertEvent converts a single OpenLineage JSON object (as raw bytes) into
// the appropriate proto OpenLineageEvent wrapper. The original JSON is preserved
// in the raw_json field for auditability.
func ConvertEvent(data []byte) (*lineagev1.OpenLineageEvent, error) {
	var raw rawEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	switch classifyEvent(&raw) {
	case eventTypeRun:
		re, err := convertRunEvent(&raw, data)
		if err != nil {
			return nil, err
		}
		return &lineagev1.OpenLineageEvent{
			Event: &lineagev1.OpenLineageEvent_RunEvent{RunEvent: re},
		}, nil

	case eventTypeJob:
		je, err := convertJobEvent(&raw, data)
		if err != nil {
			return nil, err
		}
		return &lineagev1.OpenLineageEvent{
			Event: &lineagev1.OpenLineageEvent_JobEvent{JobEvent: je},
		}, nil

	case eventTypeDataset:
		de, err := convertDatasetEvent(&raw, data)
		if err != nil {
			return nil, err
		}
		return &lineagev1.OpenLineageEvent{
			Event: &lineagev1.OpenLineageEvent_DatasetEvent{DatasetEvent: de},
		}, nil

	default:
		return nil, fmt.Errorf("unable to classify event: must contain run+job, job, or dataset fields")
	}
}

// ConvertBatch converts a JSON array of OpenLineage events.
func ConvertBatch(data []byte) ([]*lineagev1.OpenLineageEvent, error) {
	var rawItems []json.RawMessage
	if err := json.Unmarshal(data, &rawItems); err != nil {
		return nil, fmt.Errorf("invalid JSON array: %w", err)
	}

	events := make([]*lineagev1.OpenLineageEvent, 0, len(rawItems))
	for i, item := range rawItems {
		evt, err := ConvertEvent([]byte(item))
		if err != nil {
			return nil, fmt.Errorf("event[%d]: %w", i, err)
		}
		events = append(events, evt)
	}
	return events, nil
}

type eventKind int

const (
	eventTypeUnknown eventKind = iota
	eventTypeRun
	eventTypeJob
	eventTypeDataset
)

// classifyEvent determines the OpenLineage event type based on field presence:
//   - run + job  -> RunEvent
//   - dataset (no run) -> DatasetEvent
//   - job (no run) -> JobEvent
func classifyEvent(raw *rawEvent) eventKind {
	hasRun := raw.Run != nil
	hasJob := raw.Job != nil
	hasDataset := raw.Dataset != nil

	switch {
	case hasRun && hasJob:
		return eventTypeRun
	case hasDataset && !hasRun:
		return eventTypeDataset
	case hasJob && !hasRun:
		return eventTypeJob
	default:
		return eventTypeUnknown
	}
}

func convertRunEvent(raw *rawEvent, originalJSON []byte) (*lineagev1.RunEvent, error) {
	ts, err := parseEventTime(raw.EventTime)
	if err != nil {
		return nil, err
	}

	re := &lineagev1.RunEvent{
		EventType: raw.EventType,
		EventTime: ts,
		Producer:  raw.Producer,
		SchemaUrl: raw.SchemaURL,
		RawJson:   string(originalJSON),
	}

	if raw.Run != nil {
		re.Run, err = convertRun(*raw.Run)
		if err != nil {
			return nil, fmt.Errorf("run: %w", err)
		}
	}
	if raw.Job != nil {
		re.Job, err = convertJob(*raw.Job)
		if err != nil {
			return nil, fmt.Errorf("job: %w", err)
		}
	}
	re.Inputs, err = convertInputDatasets(raw.Inputs)
	if err != nil {
		return nil, err
	}
	re.Outputs, err = convertOutputDatasets(raw.Outputs)
	if err != nil {
		return nil, err
	}
	return re, nil
}

func convertJobEvent(raw *rawEvent, originalJSON []byte) (*lineagev1.JobEvent, error) {
	ts, err := parseEventTime(raw.EventTime)
	if err != nil {
		return nil, err
	}

	je := &lineagev1.JobEvent{
		EventTime: ts,
		Producer:  raw.Producer,
		SchemaUrl: raw.SchemaURL,
		RawJson:   string(originalJSON),
	}

	if raw.Job != nil {
		je.Job, err = convertJob(*raw.Job)
		if err != nil {
			return nil, fmt.Errorf("job: %w", err)
		}
	}
	je.Inputs, err = convertInputDatasets(raw.Inputs)
	if err != nil {
		return nil, err
	}
	je.Outputs, err = convertOutputDatasets(raw.Outputs)
	if err != nil {
		return nil, err
	}
	return je, nil
}

func convertDatasetEvent(raw *rawEvent, originalJSON []byte) (*lineagev1.DatasetEvent, error) {
	ts, err := parseEventTime(raw.EventTime)
	if err != nil {
		return nil, err
	}

	de := &lineagev1.DatasetEvent{
		EventTime: ts,
		Producer:  raw.Producer,
		SchemaUrl: raw.SchemaURL,
		RawJson:   string(originalJSON),
	}

	if raw.Dataset != nil {
		de.Dataset, err = convertStaticDataset(*raw.Dataset)
		if err != nil {
			return nil, fmt.Errorf("dataset: %w", err)
		}
	}
	return de, nil
}

func convertRun(data json.RawMessage) (*lineagev1.Run, error) {
	var r rawRun
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	run := &lineagev1.Run{RunId: r.RunID}
	if r.Facets != nil {
		s, err := toStruct(r.Facets)
		if err != nil {
			return nil, fmt.Errorf("facets: %w", err)
		}
		run.Facets = s
	}
	return run, nil
}

func convertJob(data json.RawMessage) (*lineagev1.Job, error) {
	var j rawJob
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	job := &lineagev1.Job{Namespace: j.Namespace, Name: j.Name}
	if j.Facets != nil {
		s, err := toStruct(j.Facets)
		if err != nil {
			return nil, fmt.Errorf("facets: %w", err)
		}
		job.Facets = s
	}
	return job, nil
}

func convertStaticDataset(data json.RawMessage) (*lineagev1.StaticDataset, error) {
	var d rawDataset
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	ds := &lineagev1.StaticDataset{Namespace: d.Namespace, Name: d.Name}
	if d.Facets != nil {
		s, err := toStruct(d.Facets)
		if err != nil {
			return nil, fmt.Errorf("facets: %w", err)
		}
		ds.Facets = s
		ds.ColumnLineage = parseColumnLineageFacet(d.Facets)
	}
	return ds, nil
}

func convertInputDatasets(raw []json.RawMessage) ([]*lineagev1.InputDataset, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]*lineagev1.InputDataset, 0, len(raw))
	for i, item := range raw {
		var d rawDataset
		if err := json.Unmarshal(item, &d); err != nil {
			return nil, fmt.Errorf("inputs[%d]: %w", i, err)
		}
		ds := &lineagev1.InputDataset{Namespace: d.Namespace, Name: d.Name}
		if d.Facets != nil {
			s, err := toStruct(d.Facets)
			if err != nil {
				return nil, fmt.Errorf("inputs[%d].facets: %w", i, err)
			}
			ds.Facets = s
			ds.ColumnLineage = parseColumnLineageFacet(d.Facets)
		}
		if d.InputFacets != nil {
			s, err := toStruct(d.InputFacets)
			if err != nil {
				return nil, fmt.Errorf("inputs[%d].inputFacets: %w", i, err)
			}
			ds.InputFacets = s
		}
		out = append(out, ds)
	}
	return out, nil
}

func convertOutputDatasets(raw []json.RawMessage) ([]*lineagev1.OutputDataset, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]*lineagev1.OutputDataset, 0, len(raw))
	for i, item := range raw {
		var d rawDataset
		if err := json.Unmarshal(item, &d); err != nil {
			return nil, fmt.Errorf("outputs[%d]: %w", i, err)
		}
		ds := &lineagev1.OutputDataset{Namespace: d.Namespace, Name: d.Name}
		if d.Facets != nil {
			s, err := toStruct(d.Facets)
			if err != nil {
				return nil, fmt.Errorf("outputs[%d].facets: %w", i, err)
			}
			ds.Facets = s
			ds.ColumnLineage = parseColumnLineageFacet(d.Facets)
		}
		if d.OutputFacets != nil {
			s, err := toStruct(d.OutputFacets)
			if err != nil {
				return nil, fmt.Errorf("outputs[%d].outputFacets: %w", i, err)
			}
			ds.OutputFacets = s
		}
		out = append(out, ds)
	}
	return out, nil
}

// parseColumnLineageFacet pulls the OpenLineage `columnLineage` facet out of a
// raw facets map and lifts it into a typed `ColumnLineageDatasetFacet` proto.
//
// The same data is left in `facets` (as a `google.protobuf.Struct`) so older
// consumers that only know about the JSON shape continue to work. The typed
// field gives newer consumers (the Rust sidecar, Spark plugin) a structured
// view without re-parsing arbitrary JSON.
//
// Returns nil when:
//   - `raw` doesn't contain a `columnLineage` key (the common case for
//     pre-column-lineage senders), or
//   - the `columnLineage` value is not an object, or
//   - the parsed facet is empty (no fields and no dataset entries).
func parseColumnLineageFacet(raw map[string]interface{}) *lineagev1.ColumnLineageDatasetFacet {
	if raw == nil {
		return nil
	}
	cl, ok := raw["columnLineage"]
	if !ok {
		return nil
	}
	return parseColumnLineage(cl)
}

func parseColumnLineage(v interface{}) *lineagev1.ColumnLineageDatasetFacet {
	obj, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	facet := &lineagev1.ColumnLineageDatasetFacet{}

	if fieldsRaw, ok := obj["fields"].(map[string]interface{}); ok && len(fieldsRaw) > 0 {
		facet.Fields = make(map[string]*lineagev1.OutputFieldLineage, len(fieldsRaw))
		for k, val := range fieldsRaw {
			if of := parseOutputFieldLineage(val); of != nil {
				facet.Fields[k] = of
			}
		}
	}

	if datasetRaw, ok := obj["dataset"].([]interface{}); ok {
		for _, d := range datasetRaw {
			if in := parseInputField(d); in != nil {
				facet.Dataset = append(facet.Dataset, in)
			}
		}
	}

	if len(facet.Fields) == 0 && len(facet.Dataset) == 0 {
		return nil
	}
	return facet
}

func parseOutputFieldLineage(v interface{}) *lineagev1.OutputFieldLineage {
	obj, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	out := &lineagev1.OutputFieldLineage{}
	if inputs, ok := obj["inputFields"].([]interface{}); ok {
		for _, i := range inputs {
			if in := parseInputField(i); in != nil {
				out.InputFields = append(out.InputFields, in)
			}
		}
	}
	if td, ok := obj["transformationDescription"].(string); ok {
		out.TransformationDescription = td
	}
	if tt, ok := obj["transformationType"].(string); ok {
		out.TransformationType = tt
	}
	if len(out.InputFields) == 0 && out.TransformationDescription == "" && out.TransformationType == "" {
		return nil
	}
	return out
}

func parseInputField(v interface{}) *lineagev1.InputField {
	obj, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	ns, _ := obj["namespace"].(string)
	nm, _ := obj["name"].(string)
	fl, _ := obj["field"].(string)
	// The OpenLineage spec marks all three of these as required on InputField.
	// If any are missing we drop the entry rather than emit a partially-formed
	// proto that downstream readers would have to defend against.
	if ns == "" || nm == "" || fl == "" {
		return nil
	}
	in := &lineagev1.InputField{
		Namespace: ns,
		Name:      nm,
		Field:     fl,
	}
	if tx, ok := obj["transformations"].([]interface{}); ok {
		for _, t := range tx {
			if ft := parseFieldTransformation(t); ft != nil {
				in.Transformations = append(in.Transformations, ft)
			}
		}
	}
	return in
}

func parseFieldTransformation(v interface{}) *lineagev1.FieldTransformation {
	obj, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	t, _ := obj["type"].(string)
	if t == "" {
		// `type` is the only required field on a transformation per the
		// OpenLineage spec; anything without it is dropped.
		return nil
	}
	ft := &lineagev1.FieldTransformation{Type: t}
	if s, ok := obj["subtype"].(string); ok {
		ft.Subtype = s
	}
	if d, ok := obj["description"].(string); ok {
		ft.Description = d
	}
	if m, ok := obj["masking"].(bool); ok {
		ft.Masking = m
	}
	return ft
}

func parseEventTime(s string) (*timestamppb.Timestamp, error) {
	if s == "" {
		return nil, fmt.Errorf("eventTime is required")
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("eventTime: %w", err)
	}
	return timestamppb.New(t), nil
}

// toStruct converts an arbitrary map to a protobuf Struct. This handles the
// OpenLineage facet maps that can contain underscore-prefixed keys and
// arbitrary nested objects.
func toStruct(m map[string]interface{}) (*structpb.Struct, error) {
	return structpb.NewStruct(m)
}
