package pkg


// Node 是数据流处理节点的通用接口
// 任何实现了 In/Out/Start/Stop 的处理单元都可以作为 Node
// 例如 Processor、MapNode、FilterNode 等
type Node interface {
	In() chan<- DU
	Out() <-chan DU
	Start()
	Stop()
}

// Pipeline 负责组装和运行 DAG
// 支持节点注册、连接、统一启动和停止

type Pipeline struct {
	nodes []Node
}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) AddNode(node Node) {
	p.nodes = append(p.nodes, node)
}

// Connect 将 from 的输出连接到 to 的输入
func (p *Pipeline) Connect(from, to Node) {
	p.addIfNotExists(from)
	p.addIfNotExists(to)
	go func() {
		for du := range from.Out() {
			to.In() <- du
		}
	}()
}

func (p *Pipeline) addIfNotExists(node Node) {
	for _, n := range p.nodes {
		if n == node {
			return
		}
	}
	p.nodes = append(p.nodes, node)
}

func (p *Pipeline) Start() {
	for _, node := range p.nodes {
		node.Start()
	}
}

func (p *Pipeline) Stop() {
	for _, node := range p.nodes {
		node.Stop()
	}
}
