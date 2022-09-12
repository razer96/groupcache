package consistenthash

import (
	"fmt"
	"github.com/segmentio/fasthash/fnv1"
	"sort"
	"strconv"
	"sync"
)

const (
	minReplicas = 100
	prime       = 16777619
)

var (
	defaultHashFunc = fnv1.HashBytes64
)

type VNConsistentHash struct {
	hashFunc Hash
	replicas int
	keys     []uint64
	ring     map[uint64][]string
	nodes    map[string]struct{}
	lock     sync.RWMutex
}

func NewVNConsistentHash() *VNConsistentHash {
	return NewCustomVNConsistentHash(minReplicas, defaultHashFunc)
}

func NewCustomVNConsistentHash(replicas int, fn Hash) *VNConsistentHash {
	if replicas < minReplicas {
		replicas = minReplicas
	}

	if fn == nil {
		fn = defaultHashFunc
	}

	return &VNConsistentHash{
		hashFunc: fn,
		replicas: replicas,
		ring:     make(map[uint64][]string),
		nodes:    make(map[string]struct{}),
	}
}

func (h *VNConsistentHash) Add(node string) {
	h.AddWithReplicas(node, h.replicas)
}

func (h *VNConsistentHash) AddWithReplicas(node string, replicas int) {
	h.Remove(node)

	if replicas > h.replicas {
		replicas = h.replicas
	}

	h.lock.Lock()
	defer h.lock.Unlock()

	h.addNode(node)

	for i := 0; i < replicas; i++ {
		hash := h.hashFunc([]byte(node + strconv.Itoa(i)))
		h.keys = append(h.keys, hash)
		h.ring[hash] = append(h.ring[hash], node)
	}

	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *VNConsistentHash) Get(key string) (string, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	if len(h.ring) == 0 {
		return "", false
	}

	hash := h.hashFunc([]byte(key))
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)

	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		return "", false
	case 1:
		return nodes[0], true
	default:
		innerIndex := h.hashFunc([]byte(innerRepr(key)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

func innerRepr(node string) string {
	return fmt.Sprintf("%d:%s", prime, node)
}

func (h *VNConsistentHash) addNode(node string) {
	h.nodes[node] = struct{}{}
}

func (h *VNConsistentHash) IsEmpty() bool {
	h.lock.RLock()
	defer h.lock.RUnlock()

	return len(h.keys) == 0
}

func (h *VNConsistentHash) Remove(node string) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if !h.containsNode(node) {
		return
	}

	for i := 0; i < h.replicas; i++ {
		hash := h.hashFunc([]byte(node + strconv.Itoa(i)))
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})
		if index < len(h.keys) && h.keys[index] == hash {
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		h.removeRingNode(hash, node)
	}

	h.removeNode(node)
}

func (h *VNConsistentHash) removeNode(node string) {
	delete(h.nodes, node)
}

func (h *VNConsistentHash) removeRingNode(hash uint64, node string) {
	if nodes, ok := h.ring[hash]; ok {
		newNodes := nodes[:0]

		for _, x := range nodes {
			if x != node {
				newNodes = append(newNodes, x)
			}
		}

		if len(newNodes) > 0 {
			h.ring[hash] = newNodes
		} else {
			delete(h.ring, hash)
		}
	}
}

func (h *VNConsistentHash) containsNode(node string) bool {
	_, ok := h.nodes[node]
	return ok
}
