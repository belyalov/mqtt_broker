package mqtt_simple

import (
	"sync"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
)

type MessageCallback func(topic, value string)

type MqttSimpleClient struct {
	client                pmqtt.Client
	opts                  *pmqtt.ClientOptions
	subscriptions         map[string][]chan *MqttMessage
	wildcardSubscriptions []chan *MqttMessage
	lock                  sync.Mutex
}

type MqttMessage struct {
	Topic string `json:"topic"`
	Value string `json:"value"`
}

func NewMqttSimpleClient(broker, clientid, user, password string) *MqttSimpleClient {

	opts := pmqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientid)
	opts.SetUsername(user)
	opts.SetPassword(password)
	opts.SetCleanSession(false)

	return &MqttSimpleClient{
		opts:          opts,
		subscriptions: make(map[string][]chan *MqttMessage),
	}
}

func (b *MqttSimpleClient) onMessage(client pmqtt.Client, pmsg pmqtt.Message) {
	// Firstly process wildcard subscribers
	msg := &MqttMessage{
		Topic: string(pmsg.Topic()),
		Value: string(pmsg.Payload()),
	}
	for _, ch := range b.wildcardSubscriptions {
		ch <- msg
	}

	// Handle specific topic subscribers
	b.lock.Lock()
	channels, ok := b.subscriptions[msg.Topic]
	b.lock.Unlock()
	if !ok {
		return
	}
	for _, ch := range channels {
		ch <- msg
	}
}

func (b *MqttSimpleClient) SetCleanSession(state bool) {
	b.opts.SetCleanSession(state)
}

func (b *MqttSimpleClient) Connect() error {

	b.opts.SetDefaultPublishHandler(b.onMessage)

	b.client = pmqtt.NewClient(b.opts)
	if token := b.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	return nil
}

func (b *MqttSimpleClient) Disconnect(timeout uint) {
	b.client.Disconnect(timeout)
}

func (b *MqttSimpleClient) Subscribe(topic string, qos byte) (<-chan *MqttMessage, error) {
	// Subscribe to given topic
	if token := b.client.Subscribe(topic, qos, nil); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	// Create channel and add it to subscribers list
	ch := make(chan *MqttMessage)
	b.lock.Lock()
	b.subscriptions[topic] = append(b.subscriptions[topic], ch)
	b.lock.Unlock()

	return ch, nil
}

func (b *MqttSimpleClient) SubscribeMultiple(topics []string, qos byte) (<-chan *MqttMessage, error) {
	// Subscribe to given topics
	topicsMap := map[string]byte{}
	for _, t := range topics {
		topicsMap[t] = qos
	}
	if token := b.client.SubscribeMultiple(topicsMap, nil); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	// Create channel and add it to subscribers list
	ch := make(chan *MqttMessage)
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, t := range topics {
		b.subscriptions[t] = append(b.subscriptions[t], ch)
	}

	return ch, nil
}

func (b *MqttSimpleClient) SubscribeToEverything(qos byte) (<-chan *MqttMessage, error) {
	// Subscribe to all topics
	if token := b.client.Subscribe("#", qos, nil); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	// Create channel and add it to wildcard subscribers list
	ch := make(chan *MqttMessage)
	b.lock.Lock()
	defer b.lock.Unlock()

	b.wildcardSubscriptions = append(b.wildcardSubscriptions, ch)

	return ch, nil
}

func (b *MqttSimpleClient) Publish(topic, value string, qos byte, retained bool) error {
	if token := b.client.Publish(topic, qos, retained, value); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}
