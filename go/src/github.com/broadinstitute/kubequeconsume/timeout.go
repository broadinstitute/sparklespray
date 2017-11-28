package kubequeconsume

import (
	"log"
	"sort"
	"strings"
	"time"

	compute "google.golang.org/api/compute/v1"
)

type Timeout interface {
	Reset(timestamp time.Time)
	HasTimeoutExpired(timestamp time.Time) bool
}

type ClusterTimeout struct {
	ClusterName string

	ResetAtLeastOnce bool

	FirstResetTimestamp time.Time
	LastResetTimestamp  time.Time

	Timeout           time.Duration
	MaxStaleHostnames time.Duration

	Service *compute.Service
	Zones   []string
	Project string
	MyName  string

	ReservationSize    int
	ReservationTimeout time.Duration

	LastHostnamePoll time.Time
	IsReservedHost   bool
}

func NewClusterTimeout(service *compute.Service, clusterName string, zones []string, project string, myname string,
	timeout time.Duration, ReservationSize int, ReservationTimeout time.Duration) *ClusterTimeout {

	// The instance name may have zone included in the path. Drop everything but the final name
	nameComponents := strings.Split(myname, "/")

	t := &ClusterTimeout{
		ClusterName:        clusterName,
		Timeout:            timeout,
		Service:            service,
		Zones:              zones,
		Project:            project,
		MyName:             nameComponents[len(nameComponents)-1],
		ReservationSize:    ReservationSize,
		ReservationTimeout: ReservationTimeout}
	t.UpdateIsReservedHost()
	return t
}

func (ct *ClusterTimeout) UpdateIsReservedHost() {
	ct.LastHostnamePoll = time.Now()

	instances, err := ct.getInstances()
	if err != nil {
		log.Printf("Got error from getInstances: %v", err)
		ct.IsReservedHost = false
		return
	}

	names := make([]string, len(instances))
	for i, instance := range instances {
		names[i] = instance.Name
	}

	log.Printf("Found %d instances: %v", len(instances), names)

	ct.IsReservedHost = isNameInTopN(names, ct.MyName, ct.ReservationSize)
}

func (ct *ClusterTimeout) Reset(timestamp time.Time) {
	if !ct.ResetAtLeastOnce {
		ct.ResetAtLeastOnce = true
		ct.FirstResetTimestamp = timestamp
	}

	ct.LastResetTimestamp = timestamp
}

func (ct *ClusterTimeout) getInstances() ([]*compute.Instance, error) {
	instances := make([]*compute.Instance, 0, 100)
	for _, zone := range ct.Zones {
		log.Printf("Querying for instances in project %s, zone %s", ct.Project, zone)
		zoneInstances, err := ct.Service.Instances.List(ct.Project, zone).Filter("labels.kubeque-cluster=" + ct.ClusterName).Do()
		if err != nil {
			return nil, err
		}
		for _, instance := range zoneInstances.Items {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func isNameInTopN(names []string, myname string, n int) bool {
	sort.Strings(names)

	nameIndex := sort.SearchStrings(names, myname)
	if nameIndex >= len(names) || names[nameIndex] != myname {
		log.Printf("Error: %s is not a running instance!", myname)
		return false
	}

	return nameIndex < n
}

func (ct *ClusterTimeout) HasTimeoutExpired(timestamp time.Time) bool {
	timeSinceLastHostCheck := timestamp.Sub(ct.LastHostnamePoll)
	if timeSinceLastHostCheck > ct.MaxStaleHostnames {
		ct.UpdateIsReservedHost()
	}

	timeSinceLastReset := timestamp.Sub(ct.LastResetTimestamp)
	if ct.IsReservedHost {
		return timeSinceLastReset > ct.ReservationTimeout
	}
	return timeSinceLastReset > ct.Timeout
}
