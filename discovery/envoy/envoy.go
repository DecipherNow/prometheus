package envoy

import (
	"context"
	"fmt"
	"strconv"

	api "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	core "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	// metaLabelPrefix is the meta prefix used for all meta labels in this discovery.
	metaLabelPrefix = model.MetaLabelPrefix + "envoy_"

	// clusterAltStatLabelName is the meta label for the alternate stat name of a cluster.
	clusterAltStatLabelName = model.LabelName(metaLabelPrefix + "cluster_alt_stat")

	// clusterLabelName is the meta label for the name of a cluster.
	clusterLabelName = model.LabelName(metaLabelPrefix + "cluster")

	// hostLabelName is the meta label for the host of a target.
	hostLabelName = model.LabelName(metaLabelPrefix + "host")

	// portLabelName is the meta label for the port of a target.
	portLabelName = model.LabelName(metaLabelPrefix + "port")

	// protocolLabelName is the meta label for the protocol of a target.
	protocolLabelName = model.LabelName(metaLabelPrefix + "protocol")
)

var (
	defaultSDConfig = SDConfig{
		ClusterID: "prometheus",
		NodeID:    "prometheus",
		Server:    "localhost:18000",
	}
)

// SDConfig represents an Envoy service discovery configuration.
type SDConfig struct {
	ClusterID string           `yaml:"cluster_id"`
	NodeID    string           `yaml:"node_id"`
	Server    string           `yaml:"server"`
	TLSConfig config.TLSConfig `yaml:"tls_config,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return errors.Wrap(err, "error unmarshalling envoy configuration")
	}
	return nil
}

// Discovery represents an Envoy service discoverer.
type Discovery struct {
	clusterID  string
	connection *grpc.ClientConn
	logger     log.Logger
	nodeID     string
}

// NewDiscovery initializes a new instance of the Discovery class.
func NewDiscovery(conf *SDConfig, logger log.Logger) (*Discovery, error) {
	tlsConfig, err := config.NewTLSConfig(&conf.TLSConfig)
	if err != nil {
		return nil, errors.Wrap(err, "error raised creating tls configuration")
	}

	dialOptions := []grpc.DialOption{
		grpc.FailOnNonTempDialError(true),
		grpc.WithBlock(),
	}

	if tlsConfig.InsecureSkipVerify {
		level.Warn(logger).Log("msg", "connection to envoy is insecure")
		dialOptions = append(dialOptions, grpc.WithInsecure())
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	connection, err := grpc.Dial(conf.Server, dialOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "error raised creating client connection")
	}

	discovery := &Discovery{
		clusterID:  conf.ClusterID,
		connection: connection,
		logger:     logger,
		nodeID:     conf.NodeID,
	}

	return discovery, nil
}

// Run the service discovery.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	client, err := discovery.NewAggregatedDiscoveryServiceClient(d.connection).StreamAggregatedResources(ctx)
	if err != nil {
		level.Error(d.logger).Log("msg", "error raised creating client", "err", err)
		return
	}

	request := &api.DiscoveryRequest{
		Node: &core.Node{
			Id:      d.nodeID,
			Cluster: d.clusterID,
		},
		ResourceNames: []string{},
		TypeUrl:       "type.googleapis.com/envoy.api.v2.Cluster",
	}

	err = client.Send(request)
	if err != nil {
		level.Error(d.logger).Log("msg", "error raised sending request", "err", err)
		return
	}

	for {
		response, err := client.Recv()
		if err != nil {
			level.Error(d.logger).Log("msg", "error raised receiving response", "err", err)
			return
		}

		resources := response.GetResources()
		targetGroups := make([]*targetgroup.Group, len(resources))

		for i, resource := range resources {
			// New pattern for unmartialling any type proto messages
			cluster := new(api.Cluster)
			err = anypb.UnmarshalTo(resource, cluster, proto.UnmarshalOptions{AllowPartial: true})
			if err != nil {
				level.Error(d.logger).Log("msg", "error raised unmarshalling resource", "err", err)
				return
			}

			targets := make([]model.LabelSet, len(cluster.Hosts))

			for j, address := range cluster.GetHosts() {
				socket := address.GetSocketAddress()
				host := socket.GetAddress()
				port := strconv.Itoa(int(socket.GetPortValue()))
				protocol := socket.GetProtocol().String()

				targets[j] = model.LabelSet{
					model.AddressLabel: model.LabelValue(fmt.Sprintf("%s:%s", host, port)),
					hostLabelName:      model.LabelValue(host),
					portLabelName:      model.LabelValue(port),
					protocolLabelName:  model.LabelValue(protocol),
				}
			}

			targetGroups[i] = &targetgroup.Group{
				Labels: model.LabelSet{
					clusterLabelName:        model.LabelValue(cluster.Name),
					clusterAltStatLabelName: model.LabelValue(cluster.AltStatName),
				},
				Source:  cluster.Name,
				Targets: targets,
			}
		}

		ch <- targetGroups

		request := &api.DiscoveryRequest{
			ResponseNonce: response.GetNonce(),
			TypeUrl:       response.GetTypeUrl(),
			VersionInfo:   response.GetVersionInfo(),
		}

		err = client.Send(request)
		if err != nil {
			level.Error(d.logger).Log("msg", "rror raised sending acknowledgement resource", "err", err)
			break
		}
	}
}
