package scalers

import (
	"context"
	"encoding/json" // For parsing to and from json (nsq responseds in json)
	"fmt"           // Used to format strings and return data in readable manner
	"io"
	"net/http" // Used for making http requests to nsq endpoints
	"net/url"  // Used for building urls for nsq endpoints
	"regexp"   // Used in logging and user defined regex
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/streadway/amqp"
	v2beta2 "k8s.io/api/autoscaling/v2beta2" // Used for the return value in GetMetricSpecForScaling() function
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"

	kedautil "github.com/kedacore/keda/v2/pkg/util" // Needed for generating the httpClient
)

// Values for defaults if not supplied
const (
	nsqQueueLengthMetricName            = "queueLength"
	nsqModeTriggerConfigName            = "mode"
	nsqValueTriggerConfigName           = "value"
	nsqActivationValueTriggerConfigName = "activationValue"
	nsqModeQueueLength                  = "QueueLength"
	nsqModeMessageRate                  = "MessageRate"
	defaultNSQQueueLength               = 20
	nsqMetricType                       = "External"
	nsqRootVhostPath                    = "/%2F"
)

// Values so we don't have to repeat type them
const (
	httpProtocol    = "http"
	amqpProtocol    = "amqp"
	autoProtocol    = "auto"
	defaultProtocol = autoProtocol
)
const (
	sumOperation     = "sum"
	avgOperation     = "avg"
	maxOperation     = "max"
	defaultOperation = sumOperation
)

// Used for logging patterns
var nsqAnonymizePattern *regexp.Regexp

// All these values are passed in via KEDA
type nsqScaler struct {
	metricType v2beta2.MetricTargetType // Should pretty much always be "nsq", fail it not
	metadata   *nsqMetadata             // The meta data block passed in the "ScaledObject" from KEDA
	connection *amqp.Connection
	channel    *amqp.Channel
	httpClient *http.Client
	logger     logr.Logger
}

type nsqMetadata struct {
	queueName             string        // NOTE: Maybe change this to "topicName" & add "channelName"?
	mode                  string        // QueueLength or MessageRate
	value                 float64       // trigger value (queue length or publish/sec. rate)
	activationValue       float64       // activation value
	host                  string        // connection string for either HTTP or AMQP protocol
	protocol              string        // either http or amqp protocol
	vhostName             string        // override the vhost from the connection info
	useRegex              bool          // specify if the queueName contains a rexeg
	excludeUnacknowledged bool          // specify if the QueueLength value should exclude Unacknowledged messages (Ready messages only)
	pageSize              int64         // specify the page size if useRegex is enabled
	operation             string        // specify the operation to apply in case of multiples queues
	metricName            string        // custom metric name for trigger
	timeout               time.Duration // custom http timeout for a specific trigger
	scalerIndex           int           // scaler index
}

// ---------- Constructor ----------- //
// This is invoked by KEDA - creates a new nsq scaler
func NewNSQScaler(config *ScalerConfig) (Scaler, error) {
	s := &nsqScaler{}

	// Assign the MetricType Value, fail if not "nsq"
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %s", err)
	}
	s.metricType = metricType

	// Tell keda to log things under this name to keep logs seperated
	s.logger = InitializeLogger(config, "nsq_scaler")

	// Parse out and assign the metadata to this new scaler
	// This will include the http client used to query the queue depth
	// Returns the "nsqMetadata" object that is assigned to `metadata`
	// NOTE: The metadata is what is passed in from the "ScaleObject" resource in kubernetes
	meta, err := parseNSQMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing nsq metadata: %s", err)
	}
	s.metadata = meta
	s.httpClient = kedautil.CreateHTTPClient(meta.timeout, false)

	if meta.protocol == amqpProtocol {
		// Override vhost if requested.
		host := meta.host
		if meta.vhostName != "" {
			hostURI, err := amqp.ParseURI(host)
			if err != nil {
				return nil, fmt.Errorf("error parsing nsq connection string: %s", err)
			}
			hostURI.Vhost = meta.vhostName
			host = hostURI.String()
		}

		conn, ch, err := getConnectionAndChannel(host)
		if err != nil {
			return nil, fmt.Errorf("error establishing nsq connection: %s", err)
		}
		s.connection = conn
		s.channel = ch
	}

	return s, nil
}
func init() {
	nsqAnonymizePattern = regexp.MustCompile(`([^ \/:]+):([^\/:]+)\@`)
}

// --- Interface Implementations ---- //
// Close disposes of NSQ connections
func (s *nsqScaler) Close(context.Context) error {
	if s.connection != nil {
		err := s.connection.Close()
		if err != nil {
			s.logger.Error(err, "Error closing nsq connection")
			return err
		}
	}
	return nil
}

// IsActive returns true if there are pending messages to be processed
func (s *nsqScaler) IsActive(ctx context.Context) (bool, error) {
	messages, publishRate, err := s.getQueueStatus()
	if err != nil {
		return false, s.anonimizeNSQError(err)
	}

	if s.metadata.mode == nsqModeQueueLength {
		return float64(messages) > s.metadata.activationValue, nil
	}
	return publishRate > s.metadata.activationValue || float64(messages) > s.metadata.activationValue, nil
}

// Returns the MetricSpec for the Horizontal Pod Autoscaler
func (s *nsqScaler) GetMetricSpecForScaling(context.Context) []v2beta2.MetricSpec {
	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.scalerIndex, s.metadata.metricName), //Per pod metric name
		},
		Target: GetMetricTargetMili(s.metricType, s.metadata.value),
	}
	metricSpec := v2beta2.MetricSpec{
		External: externalMetric, Type: nsqMetricType,
	}

	return []v2beta2.MetricSpec{metricSpec}
}

// GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *nsqScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	messages, publishRate, err := s.getQueueStatus()
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, s.anonimizeNSQError(err)
	}

	var metric external_metrics.ExternalMetricValue
	if s.metadata.mode == nsqModeQueueLength {
		metric = GenerateMetricInMili(metricName, float64(messages))
	} else {
		metric = GenerateMetricInMili(metricName, publishRate)
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

// -------- Helper Functions --------- //
// MetaData Parsing...
func parseNSQMetadata(config *ScalerConfig) (*nsqMetadata, error) {
	meta := nsqMetadata{}

	// Resolve protocol type
	meta.protocol = defaultProtocol
	if val, ok := config.AuthParams["protocol"]; ok {
		meta.protocol = val
	}
	if val, ok := config.TriggerMetadata["protocol"]; ok {
		meta.protocol = val
	}
	if meta.protocol != amqpProtocol && meta.protocol != httpProtocol && meta.protocol != autoProtocol {
		return nil, fmt.Errorf("the protocol has to be either `%s`, `%s`, or `%s` but is `%s`", amqpProtocol, httpProtocol, autoProtocol, meta.protocol)
	}

	// Resolve host value
	switch {
	case config.AuthParams["host"] != "":
		meta.host = config.AuthParams["host"]
	case config.TriggerMetadata["host"] != "":
		meta.host = config.TriggerMetadata["host"]
	case config.TriggerMetadata["hostFromEnv"] != "":
		meta.host = config.ResolvedEnv[config.TriggerMetadata["hostFromEnv"]]
	default:
		return nil, fmt.Errorf("no host setting given")
	}

	// If the protocol is auto, check the host scheme.
	if meta.protocol == autoProtocol {
		parsedURL, err := url.Parse(meta.host)
		if err != nil {
			return nil, fmt.Errorf("can't parse host to find protocol: %s", err)
		}
		switch parsedURL.Scheme {
		case "amqp", "amqps":
			meta.protocol = amqpProtocol
		case "http", "https":
			meta.protocol = httpProtocol
		default:
			return nil, fmt.Errorf("unknown host URL scheme `%s`", parsedURL.Scheme)
		}
	}

	// Resolve queueName
	if val, ok := config.TriggerMetadata["queueName"]; ok {
		meta.queueName = val
	} else {
		return nil, fmt.Errorf("no queue name given")
	}

	// Resolve vhostName
	if val, ok := config.TriggerMetadata["vhostName"]; ok {
		meta.vhostName = val
	}

	err := parseNSQHttpProtocolMetadata(config, &meta)
	if err != nil {
		return nil, err
	}

	if meta.useRegex && meta.protocol == amqpProtocol {
		return nil, fmt.Errorf("configure only useRegex with http protocol")
	}

	if meta.excludeUnacknowledged && meta.protocol == amqpProtocol {
		return nil, fmt.Errorf("configure excludeUnacknowledged=true with http protocol only")
	}

	_, err = parseTrigger(&meta, config)
	if err != nil {
		return nil, fmt.Errorf("unable to parse trigger: %s", err)
	}

	// Resolve metricName
	if val, ok := config.TriggerMetadata["metricName"]; ok {
		meta.metricName = kedautil.NormalizeString(fmt.Sprintf("nsq-%s", url.QueryEscape(val)))
	} else {
		meta.metricName = kedautil.NormalizeString(fmt.Sprintf("nsq-%s", url.QueryEscape(meta.queueName)))
	}

	// Resolve timeout
	if val, ok := config.TriggerMetadata["timeout"]; ok {
		timeoutMS, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("unable to parse timeout: %s", err)
		}
		if meta.protocol == amqpProtocol {
			return nil, fmt.Errorf("amqp protocol doesn't support custom timeouts: %s", err)
		}
		if timeoutMS <= 0 {
			return nil, fmt.Errorf("timeout must be greater than 0: %s", err)
		}
		meta.timeout = time.Duration(timeoutMS) * time.Millisecond
	} else {
		meta.timeout = config.GlobalHTTPTimeout
	}

	meta.scalerIndex = config.ScalerIndex

	return &meta, nil
}
func parseNSQHttpProtocolMetadata(config *ScalerConfig, meta *nsqMetadata) error {
	// Resolve useRegex
	if val, ok := config.TriggerMetadata["useRegex"]; ok {
		useRegex, err := strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("useRegex has invalid value")
		}
		meta.useRegex = useRegex
	}

	// Resolve excludeUnacknowledged
	if val, ok := config.TriggerMetadata["excludeUnacknowledged"]; ok {
		excludeUnacknowledged, err := strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("excludeUnacknowledged has invalid value")
		}
		meta.excludeUnacknowledged = excludeUnacknowledged
	}

	// Resolve pageSize
	if val, ok := config.TriggerMetadata["pageSize"]; ok {
		pageSize, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("pageSize has invalid value")
		}
		meta.pageSize = pageSize
		if meta.pageSize < 1 {
			return fmt.Errorf("pageSize should be 1 or greater than 1")
		}
	} else {
		meta.pageSize = 100
	}

	// Resolve operation
	meta.operation = defaultOperation
	if val, ok := config.TriggerMetadata["operation"]; ok {
		meta.operation = val
	}

	return nil
}
func parseTrigger(meta *nsqMetadata, config *ScalerConfig) (*nsqMetadata, error) {
	deprecatedQueueLengthValue, deprecatedQueueLengthPresent := config.TriggerMetadata[nsqQueueLengthMetricName]
	mode, modePresent := config.TriggerMetadata[nsqModeTriggerConfigName]
	value, valuePresent := config.TriggerMetadata[nsqValueTriggerConfigName]
	activationValue, activationValuePresent := config.TriggerMetadata[nsqActivationValueTriggerConfigName]

	// Initialize to default trigger settings
	meta.mode = nsqModeQueueLength
	meta.value = defaultNSQQueueLength

	// If nothing is specified for the trigger then return the default
	if !deprecatedQueueLengthPresent && !modePresent && !valuePresent {
		return meta, nil
	}

	// Only allow one of `queueLength` or `mode`/`value`
	if deprecatedQueueLengthPresent && (modePresent || valuePresent) {
		return nil, fmt.Errorf("queueLength is deprecated; configure only %s and %s", nsqModeTriggerConfigName, nsqValueTriggerConfigName)
	}

	// Parse activation value
	if activationValuePresent {
		activation, err := strconv.ParseFloat(activationValue, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse %s: %s", nsqActivationValueTriggerConfigName, err)
		}
		meta.activationValue = activation
	}

	// Parse deprecated `queueLength` value
	if deprecatedQueueLengthPresent {
		queueLength, err := strconv.ParseFloat(deprecatedQueueLengthValue, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse %s: %s", nsqQueueLengthMetricName, err)
		}
		meta.mode = nsqModeQueueLength
		meta.value = queueLength

		return meta, nil
	}

	if !modePresent {
		return nil, fmt.Errorf("%s must be specified", nsqModeTriggerConfigName)
	}
	if !valuePresent {
		return nil, fmt.Errorf("%s must be specified", nsqValueTriggerConfigName)
	}

	// Resolve trigger mode
	switch mode {
	case nsqModeQueueLength:
		meta.mode = nsqModeQueueLength
	case nsqModeMessageRate:
		meta.mode = nsqModeMessageRate
	default:
		return nil, fmt.Errorf("trigger mode %s must be one of %s, %s", mode, nsqModeQueueLength, nsqModeMessageRate)
	}
	triggerValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("can't parse %s: %s", nsqValueTriggerConfigName, err)
	}
	meta.value = triggerValue

	if meta.mode == nsqModeMessageRate && meta.protocol != httpProtocol {
		return nil, fmt.Errorf("protocol %s not supported; must be http to use mode %s", meta.protocol, nsqModeMessageRate)
	}

	return meta, nil
}

// Initilazing, Opening Channels
func getConnectionAndChannel(host string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(host)
	if err != nil {
		return nil, nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, channel, nil
}

// Logging
// Remove sensitive data when logging
func (s *nsqScaler) anonimizeNSQError(err error) error {
	errorMessage := fmt.Sprintf("error inspecting nsq: %s", err)
	return fmt.Errorf(nsqAnonymizePattern.ReplaceAllString(errorMessage, "user:password@"))
}

// For Queues and Queue Querying
// Will return metric data about a target queue, like message count
// Helper to determine if this needs to be de-activated or not
func (s *nsqScaler) getQueueStatus() (int64, float64, error) {
	if s.metadata.protocol == httpProtocol {
		info, err := s.getQueueInfoViaHTTP()
		if err != nil {
			return -1, -1, err
		}

		if s.metadata.excludeUnacknowledged {
			// messages count includes only ready
			return int64(info.MessagesReady), info.MessageStat.PublishDetail.Rate, nil
		}
		// messages count includes count of ready and unack-ed
		return int64(info.Messages), info.MessageStat.PublishDetail.Rate, nil
	}

	items, err := s.channel.QueueInspect(s.metadata.queueName)
	if err != nil {
		return -1, -1, err
	}

	return int64(items.Messages), 0, nil
}

// Get a json response from the target URL
func getJSON(s *nsqScaler, url string) (queueInfo, error) {
	var result queueInfo
	r, err := s.httpClient.Get(url)
	if err != nil {
		return result, err
	}
	defer r.Body.Close()

	if r.StatusCode == 200 {
		if s.metadata.useRegex {
			var queues regexQueueInfo
			err = json.NewDecoder(r.Body).Decode(&queues)
			if err != nil {
				return queueInfo{}, err
			}
			if queues.TotalPages > 1 {
				return queueInfo{}, fmt.Errorf("regex matches more queues than can be recovered at once")
			}
			result, err := getComposedQueue(s, queues.Queues)
			return result, err
		}

		err = json.NewDecoder(r.Body).Decode(&result)
		return result, err
	}

	body, _ := io.ReadAll(r.Body)
	return result, fmt.Errorf("error requesting NSQ API status: %s, response: %s, from: %s", r.Status, body, url)
}

// Getting info about the queue via an http request NOTE: This needs to be adjusted to work with nsq api endpoints
func (s *nsqScaler) getQueueInfoViaHTTP() (*queueInfo, error) {
	parsedURL, err := url.Parse(s.metadata.host)

	if err != nil {
		return nil, err
	}

	// Extract vhost from URL's path.
	vhost := parsedURL.Path

	if s.metadata.vhostName != "" {
		vhost = "/" + url.QueryEscape(s.metadata.vhostName)
	}

	if vhost == "" || vhost == "/" || vhost == "//" {
		vhost = nsqRootVhostPath
	}

	// Clear URL path to get the correct host.
	parsedURL.Path = ""

	var getQueueInfoManagementURI string
	if s.metadata.useRegex {
		getQueueInfoManagementURI = fmt.Sprintf("%s/api/queues%s?page=1&use_regex=true&pagination=false&name=%s&page_size=%d", parsedURL.String(), vhost, url.QueryEscape(s.metadata.queueName), s.metadata.pageSize)
	} else {
		getQueueInfoManagementURI = fmt.Sprintf("%s/api/queues%s/%s", parsedURL.String(), vhost, url.QueryEscape(s.metadata.queueName))
	}

	var info queueInfo
	info, err = getJSON(s, getQueueInfoManagementURI)

	if err != nil {
		return nil, err
	}

	return &info, nil
}

// Helper function to gather data about a list of queues and return it in a compiled object for easy usage
// Used as part of the `getJSON` function
func getComposedQueue(s *nsqScaler, q []queueInfo) (queueInfo, error) {
	var queue = queueInfo{}
	queue.Name = "composed-queue"
	queue.MessagesUnacknowledged = 0
	if len(q) > 0 {
		switch s.metadata.operation {
		case sumOperation:
			sumMessages, sumReady, sumRate := getSum(q)
			queue.Messages = sumMessages
			queue.MessagesReady = sumReady
			queue.MessageStat.PublishDetail.Rate = sumRate
		case avgOperation:
			avgMessages, avgReady, avgRate := getAverage(q)
			queue.Messages = avgMessages
			queue.MessagesReady = avgReady
			queue.MessageStat.PublishDetail.Rate = avgRate
		case maxOperation:
			maxMessages, maxReady, maxRate := getMaximum(q)
			queue.Messages = maxMessages
			queue.MessagesReady = maxReady
			queue.MessageStat.PublishDetail.Rate = maxRate
		default:
			return queue, fmt.Errorf("operation mode %s must be one of %s, %s, %s", s.metadata.operation, sumOperation, avgOperation, maxOperation)
		}
	} else {
		queue.Messages = 0
		queue.MessageStat.PublishDetail.Rate = 0
	}

	return queue, nil
}

// Returns the sum of messages in various states, used in `getComposedQueue`
func getSum(q []queueInfo) (int, int, float64) {
	var sumMessages int
	var sumMessagesReady int
	var sumRate float64
	for _, value := range q {
		sumMessages += value.Messages
		sumMessagesReady += value.MessagesReady
		sumRate += value.MessageStat.PublishDetail.Rate
	}
	return sumMessages, sumMessagesReady, sumRate
}

// Returns the average of messages in various states, used in `getComposedQueue`
func getAverage(q []queueInfo) (int, int, float64) {
	sumMessages, sumReady, sumRate := getSum(q)
	len := len(q)
	return sumMessages / len, sumReady / len, sumRate / float64(len)
}

// Returns the maximum of messages in various states, used in `getComposedQueue`
func getMaximum(q []queueInfo) (int, int, float64) {
	var maxMessages int
	var maxReady int
	var maxRate float64
	for _, value := range q {
		if value.Messages > maxMessages {
			maxMessages = value.Messages
		}
		if value.MessagesReady > maxReady {
			maxReady = value.MessagesReady
		}
		if value.MessageStat.PublishDetail.Rate > maxRate {
			maxRate = value.MessageStat.PublishDetail.Rate
		}
	}
	return maxMessages, maxReady, maxRate
}
