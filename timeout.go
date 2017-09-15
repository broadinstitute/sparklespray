package kubequeconsume

import (
	"log"
	"sort"
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

	Timeout time.Duration

	Service *compute.Service
	Zones   []string
	Project string
	MyName  string

	ReservationSize    int
	ReservationTimeout time.Duration
}

func NewClusterTimeout(service *compute.Service, clusterName string, zones []string, project string, myname string, timeout time.Duration,
	ReservationSize int, ReservationTimeout time.Duration) *ClusterTimeout {
	return &ClusterTimeout{
		ClusterName:        clusterName,
		Timeout:            timeout,
		Service:            service,
		Zones:              zones,
		Project:            project,
		MyName:             myname,
		ReservationSize:    ReservationSize,
		ReservationTimeout: ReservationTimeout}
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
		zoneInstances, err := ct.Service.Instances.List(ct.Project, zone).Filter("kubeque-cluster=" + ct.ClusterName).Do()
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

func hasTimeoutExpired(names []string, MyName string, timestamp time.Time, LastResetTimestamp time.Time, ReservationSize int, Timeout time.Duration, ReservationTimeout time.Duration) bool {
	timeSinceLastReset := timestamp.Sub(LastResetTimestamp)
	if isNameInTopN(names, MyName, ReservationSize) {
		return timeSinceLastReset > ReservationTimeout
	}
	return timeSinceLastReset > Timeout
}

func (ct *ClusterTimeout) HasTimeoutExpired(timestamp time.Time) bool {
	instances, err := ct.getInstances()
	if err != nil {
		log.Printf("Got error from getInstances: %v", err)
		return false
	}

	// sort the names
	names := make([]string, len(instances))
	for i, instance := range instances {
		names[i] = instance.Name
	}

	return hasTimeoutExpired(names, ct.MyName, timestamp, ct.LastResetTimestamp, ct.ReservationSize, ct.Timeout, ct.ReservationTimeout)
}
