package consul

import (
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/armon/consul-api"
	"github.com/docker/swarm/discovery"
)

type ConsulDiscoveryService struct {
	ttl    uint64
	client *consulapi.Client
	path   string
}

func init() {
	discovery.Register("consul",
		func() discovery.DiscoveryService {
			return &ConsulDiscoveryService{}
		},
	)
}

func (s *ConsulDiscoveryService) Initialize(uris string, heartbeat int) error {
	var (
		// split here because uris can contain multiples ips
		// like `consul://192.168.0.1,192.168.0.2,192.168.0.3/path`
		parts    = strings.SplitN(uris, "/", 2)
		ips      = strings.Split(parts[0], ",")
		machines []string
	)
	for _, ip := range ips {
		machines = append(machines, "http://"+ip)
	}

	config := &consulapi.Config{
	   Address: ips[0],
	}

	s.client, _ = consulapi.NewClient(config)
	kv := s.client.KV()
	s.ttl = uint64(heartbeat * 3 / 2)
	s.path = parts[1] + "/"

        kvpair := &consulapi.KVPair{Key: s.path}
        _, err := kv.Put(kvpair, nil)

        if err != nil {
                return err
        }
	
	return nil
}
func (s *ConsulDiscoveryService) Fetch() ([]*discovery.Node, error) {
	kv := s.client.KV()
	pairs, _, err := kv.List(s.path, nil)
	
	if err != nil {
		return nil, err
	}

	var nodes []*discovery.Node

	for _, pair := range pairs {
		nodes = append(nodes, discovery.NewNode(string(pair.Value)))
	}
	return nodes, nil
}

func (s *ConsulDiscoveryService) Watch() <-chan time.Time {
	timeChan := make(chan time.Time)
	go func() {
		for {
			log.Debugf("[CONSUL] Watch triggered")
			timeChan <- time.Now()
		}
	}()
	return timeChan
}

func (s *ConsulDiscoveryService) Register(addr string) error {
        kv := s.client.KV()
	p := &consulapi.KVPair{Key: s.path + addr, Value: []byte(addr)}
	_, err := kv.Put(p, nil)
	return err
}
