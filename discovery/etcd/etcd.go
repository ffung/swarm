package etcd

import (
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/go-etcd/etcd"
	"github.com/docker/swarm/discovery"
)

type EtcdDiscoveryService struct {
	ttl    uint64
	client *etcd.Client
	path   string
}

func init() {
	discovery.Register("etcd",
		func() discovery.DiscoveryService {
			return &EtcdDiscoveryService{}
		},
	)
}

func (s *EtcdDiscoveryService) Initialize(uris string, heartbeat int) error {
	var (
		// split here because uris can contain multiples ips
		// like `etcd://192.168.0.1,192.168.0.2,192.168.0.3/path`
		parts    = strings.SplitN(uris, "/", 2)
		ips      = strings.Split(parts[0], ",")
		machines []string
	)
	for _, ip := range ips {
		machines = append(machines, "http://"+ip)
	}

	s.client = etcd.NewClient(machines)
	s.ttl = uint64(heartbeat * 3 / 2)
	s.path = "/" + parts[1] + "/"
	if _, err := s.client.CreateDir(s.path, s.ttl); err != nil {
		if etcdError, ok := err.(*etcd.EtcdError); ok {
			if etcdError.ErrorCode != 105 { // skip key already exists
				return err
			}
		} else {
			return err
		}
	}
	return nil
}
func (s *EtcdDiscoveryService) Fetch() ([]*discovery.Node, error) {
	resp, err := s.client.Get(s.path, true, true)
	if err != nil {
		return nil, err
	}

	var nodes []*discovery.Node

	for _, n := range resp.Node.Nodes {
		nodes = append(nodes, discovery.NewNode(n.Value))
	}
	return nodes, nil
}

func (s *EtcdDiscoveryService) Watch() <-chan time.Time {
	watchChan := make(chan *etcd.Response)
	timeChan := make(chan time.Time)
	go s.client.Watch(s.path, 0, true, watchChan, nil)
	go func() {
		for {
			<-watchChan
			log.Debugf("[ETCD] Watch triggered")
			timeChan <- time.Now()
		}
	}()
	return timeChan
}

func (s *EtcdDiscoveryService) Register(addr string) error {
	_, err := s.client.Set(path.Join(s.path, addr), addr, s.ttl)
	return err
}
