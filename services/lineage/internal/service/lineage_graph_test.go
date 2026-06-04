package service

import (
	"testing"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func makeRunEvent(jobNS, jobName string, inputs []datasetRef, outputs []datasetRef) *lineagev1.OpenLineageEvent {
	now := timestamppb.Now()
	re := &lineagev1.RunEvent{
		EventType: "COMPLETE", EventTime: now, Producer: "test",
		Job: &lineagev1.Job{Namespace: jobNS, Name: jobName},
		Run: &lineagev1.Run{RunId: "r"},
	}
	for _, in := range inputs {
		re.Inputs = append(re.Inputs, &lineagev1.InputDataset{Namespace: in.namespace, Name: in.name})
	}
	for _, out := range outputs {
		re.Outputs = append(re.Outputs, &lineagev1.OutputDataset{Namespace: out.namespace, Name: out.name})
	}
	return &lineagev1.OpenLineageEvent{
		Event: &lineagev1.OpenLineageEvent_RunEvent{RunEvent: re},
	}
}

func TestBuildLineageGraph_SeedNotFound(t *testing.T) {
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "other", nil, nil),
	}
	nodes := buildLineageGraph(events, "ns", "missing", 1)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for missing job, got %d", len(nodes))
	}
}

func TestBuildLineageGraph_EmptyEvents(t *testing.T) {
	nodes := buildLineageGraph(nil, "ns", "job", 1)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for empty events, got %d", len(nodes))
	}
}

func TestBuildLineageGraph_JobWithNoDatasets(t *testing.T) {
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "solo-job", nil, nil),
	}
	nodes := buildLineageGraph(events, "ns", "solo-job", 1)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node (just the job), got %d", len(nodes))
	}
	if nodes[0].NodeType != "job" || nodes[0].Name != "solo-job" {
		t.Errorf("unexpected node: %+v", nodes[0])
	}
	if len(nodes[0].Edges) != 0 {
		t.Errorf("expected 0 edges, got %d", len(nodes[0].Edges))
	}
}

func TestBuildLineageGraph_Depth1(t *testing.T) {
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "job1",
			[]datasetRef{{"pg", "raw"}},
			[]datasetRef{{"s3", "curated"}},
		),
	}
	nodes := buildLineageGraph(events, "ns", "job1", 1)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	nodeMap := make(map[string]*lineagev1.LineageNode)
	for _, n := range nodes {
		nodeMap[n.NodeType+":"+n.Namespace+"/"+n.Name] = n
	}

	if _, ok := nodeMap["job:ns/job1"]; !ok {
		t.Error("missing job node")
	}
	if _, ok := nodeMap["dataset:pg/raw"]; !ok {
		t.Error("missing input dataset node")
	}
	if _, ok := nodeMap["dataset:s3/curated"]; !ok {
		t.Error("missing output dataset node")
	}
}

func TestBuildLineageGraph_Depth2_Chain(t *testing.T) {
	// jobA -> datasetX -> jobB -> datasetY -> jobC
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "jobA", nil, []datasetRef{{"w", "X"}}),
		makeRunEvent("ns", "jobB", []datasetRef{{"w", "X"}}, []datasetRef{{"w", "Y"}}),
		makeRunEvent("ns", "jobC", []datasetRef{{"w", "Y"}}, nil),
	}

	// depth=1 from jobB: jobB + X + Y
	nodes1 := buildLineageGraph(events, "ns", "jobB", 1)
	if len(nodes1) != 3 {
		t.Errorf("depth=1: expected 3 nodes, got %d", len(nodes1))
	}

	// depth=2 from jobB: jobB + X + Y + jobA + jobC
	nodes2 := buildLineageGraph(events, "ns", "jobB", 2)
	if len(nodes2) != 5 {
		t.Errorf("depth=2: expected 5 nodes, got %d", len(nodes2))
	}

	// depth=3 from jobB: still 5 since jobA has no other inputs and jobC has no other outputs
	nodes3 := buildLineageGraph(events, "ns", "jobB", 3)
	if len(nodes3) != 5 {
		t.Errorf("depth=3: expected 5 nodes (no new), got %d", len(nodes3))
	}
}

func TestBuildLineageGraph_MultipleEventsForSameJob(t *testing.T) {
	// Two events for the same job add different datasets
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "job1", []datasetRef{{"pg", "a"}}, nil),
		makeRunEvent("ns", "job1", []datasetRef{{"pg", "b"}}, []datasetRef{{"s3", "out"}}),
	}

	nodes := buildLineageGraph(events, "ns", "job1", 1)
	// job + a + b + out = 4 nodes
	if len(nodes) != 4 {
		t.Errorf("expected 4 nodes, got %d", len(nodes))
	}

	jobNode := findNode(nodes, "job", "ns", "job1")
	if jobNode == nil {
		t.Fatal("job node not found")
	}
	if len(jobNode.Edges) != 3 {
		t.Errorf("expected 3 edges (2 consumes + 1 produces), got %d", len(jobNode.Edges))
	}
}

func TestBuildLineageGraph_DatasetNodeEdges(t *testing.T) {
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "producer-job", nil, []datasetRef{{"w", "shared"}}),
		makeRunEvent("ns", "consumer-job", []datasetRef{{"w", "shared"}}, nil),
	}

	// depth=2 from producer-job: producer-job -> shared -> consumer-job
	nodes := buildLineageGraph(events, "ns", "producer-job", 2)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	dsNode := findNode(nodes, "dataset", "w", "shared")
	if dsNode == nil {
		t.Fatal("dataset node not found")
	}
	if len(dsNode.Edges) != 2 {
		t.Errorf("dataset node expected 2 edges (produced-by + consumed-by), got %d", len(dsNode.Edges))
	}
}

func TestBuildLineageGraph_SortedOutput(t *testing.T) {
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "job1",
			[]datasetRef{{"a", "z"}, {"b", "y"}, {"a", "a"}},
			[]datasetRef{{"c", "m"}},
		),
	}

	nodes := buildLineageGraph(events, "ns", "job1", 1)
	// Nodes should be sorted: datasets before jobs (alphabetically by type)
	if len(nodes) < 2 {
		t.Fatalf("expected at least 2 nodes, got %d", len(nodes))
	}
	for i := 1; i < len(nodes); i++ {
		prev := nodes[i-1]
		cur := nodes[i]
		key := func(n *lineagev1.LineageNode) string {
			return n.NodeType + ":" + n.Namespace + ":" + n.Name
		}
		if key(prev) > key(cur) {
			t.Errorf("nodes not sorted: %s > %s", key(prev), key(cur))
		}
	}

	jobNode := findNode(nodes, "job", "ns", "job1")
	if jobNode == nil {
		t.Fatal("job node not found")
	}
	for i := 1; i < len(jobNode.Edges); i++ {
		prev := jobNode.Edges[i-1]
		cur := jobNode.Edges[i]
		if prev.Relationship > cur.Relationship {
			t.Errorf("edges not sorted: %s > %s", prev.Relationship, cur.Relationship)
		}
	}
}

func TestBuildLineageGraph_JobEvent(t *testing.T) {
	now := timestamppb.Now()
	events := []*lineagev1.OpenLineageEvent{
		{Event: &lineagev1.OpenLineageEvent_JobEvent{JobEvent: &lineagev1.JobEvent{
			EventTime: now, Producer: "test",
			Job:    &lineagev1.Job{Namespace: "airflow", Name: "dag.task"},
			Inputs: []*lineagev1.InputDataset{{Namespace: "bq", Name: "raw"}},
		}}},
	}
	nodes := buildLineageGraph(events, "airflow", "dag.task", 1)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestBuildLineageGraph_SkipsDatasetEvents(t *testing.T) {
	now := timestamppb.Now()
	events := []*lineagev1.OpenLineageEvent{
		makeRunEvent("ns", "j", nil, nil),
		{Event: &lineagev1.OpenLineageEvent_DatasetEvent{DatasetEvent: &lineagev1.DatasetEvent{
			EventTime: now, Producer: "test",
			Dataset: &lineagev1.StaticDataset{Namespace: "s3", Name: "ds"},
		}}},
	}
	nodes := buildLineageGraph(events, "ns", "j", 1)
	if len(nodes) != 1 {
		t.Errorf("expected 1 node (DatasetEvent doesn't contribute to job graph), got %d", len(nodes))
	}
}

func findNode(nodes []*lineagev1.LineageNode, typ, ns, name string) *lineagev1.LineageNode {
	for _, n := range nodes {
		if n.NodeType == typ && n.Namespace == ns && n.Name == name {
			return n
		}
	}
	return nil
}
