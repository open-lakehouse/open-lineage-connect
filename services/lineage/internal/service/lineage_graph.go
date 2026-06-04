package service

import (
	"slices"
	"strings"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
)

type datasetRef struct {
	namespace string
	name      string
}

type jobRef struct {
	namespace string
	name      string
}

// jobRecord aggregates the inputs and outputs seen across all events for a job.
type jobRecord struct {
	inputs  map[datasetRef]bool
	outputs map[datasetRef]bool
}

type nodeKey struct {
	typ       string
	namespace string
	name      string
}

// buildLineageGraph walks the stored events and builds a lineage graph rooted
// at the given job, expanding outward through the job-dataset bipartite graph
// up to `depth` hops. depth=1 returns the seed job + its direct datasets,
// depth=2 adds upstream/downstream jobs, and so on.
func buildLineageGraph(events []*lineagev1.OpenLineageEvent, jobNamespace, jobName string, depth int32) []*lineagev1.LineageNode {
	jobs := indexJobRecords(events)

	seed := jobRef{jobNamespace, jobName}
	if _, ok := jobs[seed]; !ok {
		return nil
	}

	visited := make(map[nodeKey]bool)
	nodesMap := make(map[nodeKey]*lineagev1.LineageNode)

	seedKey := nodeKey{"job", seed.namespace, seed.name}
	queue := []bfsItem{{key: seedKey, depth: 0}}
	visited[seedKey] = true

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		node := &lineagev1.LineageNode{
			NodeType:  cur.key.typ,
			Namespace: cur.key.namespace,
			Name:      cur.key.name,
		}
		nodesMap[cur.key] = node

		if cur.key.typ == "job" {
			expandJobNode(node, cur, jobs, visited, &queue, depth)
		} else {
			expandDatasetNode(node, cur, jobs, visited, &queue, depth)
		}
	}

	return sortedNodes(nodesMap)
}

// indexJobRecords scans all events and builds a map of job -> {inputs, outputs}.
func indexJobRecords(events []*lineagev1.OpenLineageEvent) map[jobRef]*jobRecord {
	jobs := make(map[jobRef]*jobRecord)

	for _, evt := range events {
		var jr jobRef
		var inputs []*lineagev1.InputDataset
		var outputs []*lineagev1.OutputDataset

		switch e := evt.Event.(type) {
		case *lineagev1.OpenLineageEvent_RunEvent:
			if e.RunEvent.GetJob() == nil {
				continue
			}
			jr = jobRef{e.RunEvent.Job.Namespace, e.RunEvent.Job.Name}
			inputs = e.RunEvent.Inputs
			outputs = e.RunEvent.Outputs
		case *lineagev1.OpenLineageEvent_JobEvent:
			if e.JobEvent.GetJob() == nil {
				continue
			}
			jr = jobRef{e.JobEvent.Job.Namespace, e.JobEvent.Job.Name}
			inputs = e.JobEvent.Inputs
			outputs = e.JobEvent.Outputs
		default:
			continue
		}

		rec, ok := jobs[jr]
		if !ok {
			rec = &jobRecord{
				inputs:  make(map[datasetRef]bool),
				outputs: make(map[datasetRef]bool),
			}
			jobs[jr] = rec
		}
		for _, in := range inputs {
			rec.inputs[datasetRef{in.Namespace, in.Name}] = true
		}
		for _, out := range outputs {
			rec.outputs[datasetRef{out.Namespace, out.Name}] = true
		}
	}
	return jobs
}

type bfsItem struct {
	key   nodeKey
	depth int32
}

func expandJobNode(
	node *lineagev1.LineageNode,
	cur bfsItem,
	jobs map[jobRef]*jobRecord,
	visited map[nodeKey]bool,
	queue *[]bfsItem,
	maxDepth int32,
) {
	rec := jobs[jobRef{cur.key.namespace, cur.key.name}]
	if rec == nil {
		return
	}

	for ds := range rec.inputs {
		node.Edges = append(node.Edges, &lineagev1.LineageEdge{
			SourceNamespace: ds.namespace,
			SourceName:      ds.name,
			TargetNamespace: cur.key.namespace,
			TargetName:      cur.key.name,
			Relationship:    "consumes",
		})
		enqueueIfNew(nodeKey{"dataset", ds.namespace, ds.name}, cur.depth+1, maxDepth, visited, queue)
	}

	for ds := range rec.outputs {
		node.Edges = append(node.Edges, &lineagev1.LineageEdge{
			SourceNamespace: cur.key.namespace,
			SourceName:      cur.key.name,
			TargetNamespace: ds.namespace,
			TargetName:      ds.name,
			Relationship:    "produces",
		})
		enqueueIfNew(nodeKey{"dataset", ds.namespace, ds.name}, cur.depth+1, maxDepth, visited, queue)
	}

	sortEdges(node.Edges)
}

func expandDatasetNode(
	node *lineagev1.LineageNode,
	cur bfsItem,
	jobs map[jobRef]*jobRecord,
	visited map[nodeKey]bool,
	queue *[]bfsItem,
	maxDepth int32,
) {
	ds := datasetRef{cur.key.namespace, cur.key.name}

	for jr, rec := range jobs {
		if rec.outputs[ds] {
			node.Edges = append(node.Edges, &lineagev1.LineageEdge{
				SourceNamespace: jr.namespace,
				SourceName:      jr.name,
				TargetNamespace: ds.namespace,
				TargetName:      ds.name,
				Relationship:    "produces",
			})
			enqueueIfNew(nodeKey{"job", jr.namespace, jr.name}, cur.depth+1, maxDepth, visited, queue)
		}
		if rec.inputs[ds] {
			node.Edges = append(node.Edges, &lineagev1.LineageEdge{
				SourceNamespace: ds.namespace,
				SourceName:      ds.name,
				TargetNamespace: jr.namespace,
				TargetName:      jr.name,
				Relationship:    "consumes",
			})
			enqueueIfNew(nodeKey{"job", jr.namespace, jr.name}, cur.depth+1, maxDepth, visited, queue)
		}
	}

	sortEdges(node.Edges)
}

func enqueueIfNew(key nodeKey, nextDepth, maxDepth int32, visited map[nodeKey]bool, queue *[]bfsItem) {
	if nextDepth <= maxDepth && !visited[key] {
		visited[key] = true
		*queue = append(*queue, bfsItem{key: key, depth: nextDepth})
	}
}

func sortedNodes(m map[nodeKey]*lineagev1.LineageNode) []*lineagev1.LineageNode {
	nodes := make([]*lineagev1.LineageNode, 0, len(m))
	for _, n := range m {
		nodes = append(nodes, n)
	}
	slices.SortFunc(nodes, func(a, b *lineagev1.LineageNode) int {
		if c := strings.Compare(a.NodeType, b.NodeType); c != 0 {
			return c
		}
		if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	return nodes
}

func sortEdges(edges []*lineagev1.LineageEdge) {
	slices.SortFunc(edges, func(a, b *lineagev1.LineageEdge) int {
		if c := strings.Compare(a.Relationship, b.Relationship); c != 0 {
			return c
		}
		if c := strings.Compare(a.SourceNamespace, b.SourceNamespace); c != 0 {
			return c
		}
		if c := strings.Compare(a.SourceName, b.SourceName); c != 0 {
			return c
		}
		if c := strings.Compare(a.TargetNamespace, b.TargetNamespace); c != 0 {
			return c
		}
		return strings.Compare(a.TargetName, b.TargetName)
	})
}
