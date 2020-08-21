package kubequeconsume

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
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
	Project string
	MyName  string

	ReservationTimeout time.Duration

	client           *datastore.Client
	LastHostnamePoll time.Time
	ReservedHostName string
	deadInstances    map[string]bool
}

func NewClusterTimeout(service *compute.Service, clusterName string, project string, myname string,
	timeout time.Duration, ReservationTimeout time.Duration, client *datastore.Client) *ClusterTimeout {

	// The instance name may have zone included in the path. Drop everything but the final name
	nameComponents := strings.Split(myname, "/")

	t := &ClusterTimeout{
		ClusterName:        clusterName,
		Timeout:            timeout,
		MaxStaleHostnames:  10 * time.Second,
		Service:            service,
		Project:            project,
		MyName:             nameComponents[len(nameComponents)-1],
		ReservationTimeout: ReservationTimeout,
		client:             client,
		deadInstances:      make(map[string]bool)}
	t.UpdateReservedHost()
	return t
}

func (ct *ClusterTimeout) UpdateReservedHost() {
	ct.LastHostnamePoll = time.Now()

	firstInstanceName, err := ct.getFirstRunningInstance()
	if err != nil {
		log.Printf("Got error from getFirstRunningInstance: %v", err)
		ct.ReservedHostName = ""
		return
	}

	log.Printf("Found name of host to reserve: %s", firstInstanceName)

	ct.ReservedHostName = firstInstanceName
}

func (ct *ClusterTimeout) Reset(timestamp time.Time) {
	if !ct.ResetAtLeastOnce {
		ct.ResetAtLeastOnce = true
		ct.FirstResetTimestamp = timestamp
	}

	ct.LastResetTimestamp = timestamp
}

type NodeReqList []*NodeReq

func (nr NodeReqList) Len() int {
	return len(nr)
}
func (nr NodeReqList) Less(i, j int) bool {
	return nr[i].Sequence < nr[j].Sequence
}

func (nr NodeReqList) Swap(i, j int) {
	t := nr[i]
	nr[i] = nr[j]
	nr[j] = t
}

func (ct *ClusterTimeout) getFirstRunningInstance() (string, error) {
	// find all node requests which have not yet been marked complete
	// TODO: Check this is working
	q := datastore.NewQuery("NodeReq").Filter("cluster_id =", ct.ClusterName)
	var nodeReqs []*NodeReq
	_, err := ct.client.GetAll(context.Background(), q, &nodeReqs)

	if err != nil {
		return "", err
	}

	sort.Sort(NodeReqList(nodeReqs))

	log.Printf("getFirstRunningInstance checking %d NodeReqs", len(nodeReqs))
	for _, nodeReq := range nodeReqs {
		nameAndZone := fmt.Sprintf("%s/%s", nodeReq.Zone, nodeReq.InstanceName)

		if nodeReq.InstanceName == "" || nodeReq.Zone == "" || (nodeReq.InstanceName != ct.MyName && nodeReq.Status != "running") || ct.deadInstances[nameAndZone] {
			// skip requests which are missing an instance name or instances that we've already checked and are dead
			log.Printf("Skipping instance \"%s\" (zone: %s, status: %s), ct.deadInstances[nameAndZone] = %v ", nodeReq.InstanceName, nodeReq.Zone, nodeReq.Status, ct.deadInstances[nameAndZone])
			continue
		}

		instance, err := ct.Service.Instances.Get(ct.Project, nodeReq.Zone, nodeReq.InstanceName).Do()
		// what is the error returned when an instance doesn't exist?
		// remember this instance is dead so we don't keep rechecking
		// what is the status of an instance stopped/running?

		isDead := instance != nil && (instance.Status == "TERMINATED" || instance.Status == "STOPPING" || instance.Status == "STOPPED")

		if IsNotFound(err) || isDead {
			// remember this instance isn't running to avoid polling it again
			ct.deadInstances[nameAndZone] = true
			log.Printf("Instance %s (zone: %s) is no longer running", nodeReq.InstanceName, nodeReq.Zone)
		}

		if err == nil {
			if instance.Status == "RUNNING" {
				return instance.Name, nil
			} else {
				log.Printf("instance.Status = %s", instance.Status)
			}
		} else {
			log.Printf("Got error fetching instance name:%s zone: %s: %v", nodeReq.InstanceName, nodeReq.Zone, err)
		}
	}

	log.Printf("Could not find running instance")
	return "", nil
}

func IsNotFound(err error) bool {
	ae, ok := err.(*googleapi.Error)
	return ok && ae.Code == http.StatusNotFound

}

func (ct *ClusterTimeout) HasTimeoutExpired(timestamp time.Time) bool {
	timeSinceLastHostCheck := timestamp.Sub(ct.LastHostnamePoll)
	if timeSinceLastHostCheck > ct.MaxStaleHostnames || ct.ReservedHostName == "" {
		ct.UpdateReservedHost()
	}

	timeSinceLastReset := timestamp.Sub(ct.LastResetTimestamp)
	if ct.ReservedHostName == ct.MyName {
		return timeSinceLastReset > ct.ReservationTimeout
	}
	return timeSinceLastReset > ct.Timeout
}
