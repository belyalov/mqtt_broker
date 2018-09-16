package mqtt_simple

import (
	pmqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/net/context"
)

type MqttSimple struct {
	client        pmqtt.Client
	opts          *pmqtt.ClientOptions
	subscriptions map[string][]chan string
}

func NewMqttSimple(broker, clientid, user, password string) *MqttSimple {

	opts := pmqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientid)
	opts.SetUsername(user)
	opts.SetPassword(password)
	opts.SetCleanSession(true)

	return &MqttSimple{
		opts:          opts,
		subscriptions: make(map[string][]chan string),
	}
}

func (b *MqttSimple) onMessage(client pmqtt.Client, msg pmqtt.Message) {
	payload := string(msg.Payload())
	// Broadcast message to all subscribers
	if channels, ok := b.subscriptions[msg.Topic()]; ok {
		for _, ch := range channels {
			ch <- payload
		}
	}
}

func (b *MqttSimple) Start(ctx context.Context) error {

	b.opts.SetDefaultPublishHandler(b.onMessage)

	b.client = pmqtt.NewClient(b.opts)
	if token := b.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	// To be able to disconnect client whenever context is done
	go func() {
		<-ctx.Done()
		b.client.Disconnect(100)
	}()

	return nil
}

func (b *MqttSimple) Subscribe(topic string, qos byte) (<-chan string, error) {
	// Create channel and add it to subscribers list
	ch := make(chan string, 1)
	b.subscriptions[topic] = append(b.subscriptions[topic], ch)

	// Subscribe to given topic
	if token := b.client.Subscribe(topic, qos, nil); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return ch, nil
}

func (b *MqttSimple) Publish(topic, value string, qos byte, retained bool) error {
	if token := b.client.Publish(topic, qos, retained, value); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}
