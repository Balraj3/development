package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	toolsWatch "k8s.io/client-go/tools/watch"
	"log"
	"os"
	"regexp"
	kv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"strconv"
	"strings"
	"time"
)

type logEvent struct {
	NodePool             string `json:"nodepool"`
	NodeClaim            string `json:"nodeclaim"`
	NodeClaimCpuRequests int64  `json:"nodeClaimCpuRequests"`
	NodeClaimMemRequests int64  `json:"nodeClaimMemRequests"`
	Node                 string `json:"node"`
	NodeCapacityType     string `json:"nodeCapacityType"`
	InstanceType         string `json:"instanceType"`
	NodeCpuAllocatable   int64  `json:"nodeCpuAllocatable"`
	NodeMemAllocatable   int64  `json:"nodeMemAllocatable"`
}

var dynamicClient *dynamic.DynamicClient
var gvr schema.GroupVersionResource

func checkNodeClaimCondition(nodeClaim *unstructured.Unstructured) (nc *unstructured.Unstructured, err error) {

	for {
		nodeClaimName := extractField(nodeClaim.Object, "metadata,name", ",")

		select {

		case <-time.After(5 * time.Minute):
			log.Printf("Timeout reached while checking conditions for nodeclaim %v\n", nodeClaimName)

		default:

			log.Printf("Checking Conditions for nodeclaim  %v\n", nodeClaimName)

			nc, err := dynamicClient.Resource(gvr).Get(context.TODO(), nodeClaimName, v1.GetOptions{})

			if err != nil {
				log.Printf("Error getting nodeclaim %v: %v\n", nodeClaimName, err)
				return nil, err
			}

			b, err := nc.MarshalJSON()

			if err != nil {
				log.Println("Error in marshalling ", err)
			}

			var nodeclaim kv1.NodeClaim

			err = json.Unmarshal(b, &nodeclaim)

			if err != nil {
				log.Println("Error in unmarshalling ", err)
			}

			for _, condition := range nodeclaim.GetConditions() {

				if condition.Type == "Ready" && condition.Status == "True" {
					log.Printf("Nodeclaim %v is Ready \n", nodeClaimName)
					return nc, nil
				}

			}

			log.Printf("Nodeclaim %v is Not Ready \n", nodeClaimName)

			time.Sleep(10 * time.Duration(time.Second))

		}
	}

}

var cwClient *cloudwatchlogs.Client
var logGroupName, logStreamName string

func NewLogEvent(nodeClaim *unstructured.Unstructured) logEvent {

	log.Printf("Generating new CW log event for nodeclaim %v\n", extractField(nodeClaim.Object, "metadata,name", ","))

	nodeClaimCpuRequests := convertToInt(extractField(nodeClaim.Object, "spec,resources,requests,cpu", ","))
	nodeClaimMemRequests := convertToInt(extractField(nodeClaim.Object, "spec,resources,requests,memory", ","))
	node := extractField(nodeClaim.Object, "status,nodeName", ",")
	nodeCpuAllocatable := convertToInt(extractField(nodeClaim.Object, "status,allocatable,cpu", ","))
	nodeMemAllocatable := convertToInt(extractField(nodeClaim.Object, "status,allocatable,memory", ","))
	nodePool := nodeClaim.GetLabels()["karpenter.sh/nodepool"]
	instanceType := nodeClaim.GetLabels()["node.kubernetes.io/instance-type"]
	nodeCapacityType := nodeClaim.GetLabels()["karpenter.sh/capacity-type"]

	// Create logEvent instance
	event := logEvent{
		NodePool:             nodePool,
		NodeClaim:            nodeClaim.GetName(),
		Node:                 node,
		NodeClaimCpuRequests: nodeClaimCpuRequests,
		NodeClaimMemRequests: nodeClaimMemRequests,
		NodeCpuAllocatable:   nodeCpuAllocatable,
		NodeMemAllocatable:   nodeMemAllocatable,
		InstanceType:         instanceType,
		NodeCapacityType:     nodeCapacityType,
	}

	return event

}

func convertToInt(str string) int64 {
	re := regexp.MustCompile(`[a-zA-Z]`)
	cleanedStr := re.ReplaceAllString(str, "")
	c, err := strconv.Atoi(cleanedStr)

	if err != nil {
		log.Printf("Error in converting fields: %v\n", err)
		return 0

	}
	return int64(c)
}

func extractField(obj map[string]interface{}, fieldPath string, separator string) string {

	// Split the fieldPath using the provided separator
	fields := strings.Split(fieldPath, separator)

	// Use unstructured.NestedString to access the nested field
	field, found, err := unstructured.NestedString(obj, fields...)
	if err != nil {
		log.Printf("Error accessing %v: %v\n", fields, err)
	} else if !found {
		log.Printf("Field not found for path: %v for nodeclaim\n", fieldPath)
	} else {
		return field
	}
	return ""
}

func publishLogEvent(logEvent logEvent) error {

	logData, err := json.Marshal(logEvent)
	if err != nil {
		return fmt.Errorf("unable to marshal log event, %v", err)
	}

	//put log event

	_, err = cwClient.PutLogEvents(context.TODO(), &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
		LogEvents: []types.InputLogEvent{
			{
				Message:   aws.String(string(logData)),
				Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to put log event, %v", err)
	}

	return nil

}

func setUpCW() (err error) {

	log.Println("Setting up cloudwatch....")
	cfg, err := config.LoadDefaultConfig(context.TODO())

	if err != nil {
		return fmt.Errorf("unable to load SDK config, %v", err)
	}

	//create CW client

	cwClient = cloudwatchlogs.NewFromConfig(cfg)

	logGroupName = os.Getenv("CW_LOG_GROUP_NAME")

	if logGroupName == "" {
		return fmt.Errorf("CW_LOG_GROUP_NAME environment variable is not set")
	}

	logStreamName = os.Getenv("CW_LOG_STREAM_NAME")
	if logStreamName == "" {
		return fmt.Errorf("CW_LOG_STREAM_NAME environment variable is not set")
	}

	//create log group
	_, err = cwClient.CreateLogGroup(context.TODO(), &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(logGroupName),
	})

	if err != nil {
		// If the group already exists, we can ignore this error
		var alreadyExists *types.ResourceAlreadyExistsException
		if !errors.As(err, &alreadyExists) {
			return fmt.Errorf("failed to create log group, %v", err)
		}
	}

	_, err = cwClient.CreateLogStream(context.TODO(), &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
	})
	if err != nil {
		// If the stream already exists, we can ignore this error
		var alreadyExists *types.ResourceAlreadyExistsException
		if !errors.As(err, &alreadyExists) {
			return fmt.Errorf("failed to create log stream, %v", err)
		}
	}

	return nil

}

func main() {
	//Load kubeconfig
	//creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Create a dynamic client for CRDs
	dynamicClient, err = dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating dynamic client: %v", err)
	}

	err = setUpCW()

	if err != nil {
		log.Fatalf("Error setting up CloudWatch: %v", err)
	}

	// Define the GVR for NodeClaim
	gvr = schema.GroupVersionResource{
		Group:    "karpenter.sh",
		Version:  "v1beta1",
		Resource: "nodeclaims", // Resource name
	}

	// Watch NodeClaim resources
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchFunc := func(options v1.ListOptions) (watch.Interface, error) {
		// timeOut := int64(60)
		return dynamicClient.Resource(gvr).Watch(ctx, v1.ListOptions{})
	}

	watcher, err := toolsWatch.NewRetryWatcher("1", &cache.ListWatch{WatchFunc: watchFunc})

	if err != nil {
		log.Fatalf("Error creating watcher: %v", err)
	}

	// Handle watch events
	go func() {
		for event := range watcher.ResultChan() {
			nodeClaim, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				log.Println("Unexpected type")
				continue
			}

			switch event.Type {
			case watch.Added:

				//check if nodeclaim object is ready

				nc, err := checkNodeClaimCondition(nodeClaim)

				if err != nil {
					log.Printf("Error in checking nodeclaim condition: %v\n", err)

				} else {

				// Use the constructor to create logEvent
				logEvent := NewLogEvent(nc)

				err = publishLogEvent(logEvent)

				if err != nil {
					log.Printf("Error in publishing log event: %v\n", err)
				}

				data, err := json.Marshal(logEvent)
				if err != nil {
					log.Printf("Error in Marshaling struct: %v\n", err)
				}
				log.Println(string(data))
			}

			}
		}
	}()

	// Keep the program running to watch for events
	log.Println("Watching for NodeClaim events...")
	select {}
}
