<?php


namespace Tochka\MQAdapter\Protocols;

use AMQPChannel;
use AMQPConnection;
use AMQPEnvelope;
use AMQPExchange;
use AMQPQueue;
use AMQPQueueException;
use AMQPException;
use AMQPBasicProperties;
use Tochka\MQAdapter\Exceptions\MqAdapterException;

/**
 * Class Amqp
 * @package Tochka\MQAdapter\Protocols
 */
class Amqp
{
    public const CONNECT_TYPE_PUBLISH = 'publish';
    public const CONNECT_TYPE_CONSUME = 'consume';
    public const PUBLISH_CONFIRM_WAIT = 0.4;

    protected $hosts;
    protected $login;
    protected $password;
    protected $settings;
    /**
     * @var AMQPConnection $connection
     */
    protected $connection;
    /**
     * @var AMQPChannel $channel
     */
    protected $channel;
    protected $queues;
    protected $currentQueue;
    protected $connectType = self::CONNECT_TYPE_CONSUME;

    /**
     * AmqpAdapter constructor.
     *
     * @param $hosts
     * @param $login
     * @param $password
     * @param $settings
     */
    public function __construct($hosts, $login, $password, $settings)
    {
        $this->hosts = $hosts;
        $this->login = $login;
        $this->password = $password;
        $this->settings = $settings;
    }

    public function setConnectTypePublish(): void
    {
        $this->connectType = self::CONNECT_TYPE_PUBLISH;
    }

    /**
     * При уничтожении объекта - отключаемся
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Проверка подключения к посреднику
     */
    public function checkConnection(): bool
    {
        if (!$this->isConnected() || $this->hasErrors()) {
            return $this->reconnect();
        }

        return true;
    }

    /**
     * Выполняет отключение
     */
    public function disconnect(): void
    {
        if ($this->isConnected()) {
            $this->channel->close();
            $this->connection->disconnect();
        }
        unset($this->channel, $this->connection);
    }

    /**
     * @param string $destination
     * @param string $message
     * @param array  $settings
     *
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     * @throws \AMQPException
     */
    public function send(string $destination, string $message, array $settings = []): bool
    {
        $this->setConnectTypePublish();

        $this->checkConnection();

        $this->setPublisherConfirms();

        $exchange = new AMQPExchange($this->channel);
        $exchange->setName($destination);
        $result = $exchange->publish($message, '', AMQP_NOPARAM, $settings);
        if (!$result) {
            throw new AMQPException('Failed to publish content.');
        }

        try {
            $this->channel->waitForConfirm(self::PUBLISH_CONFIRM_WAIT);
        } catch (AMQPException $e) {
            if ((int)$e->getCode() !== 0) {
                throw $e;
            }
        }

        return $result;
    }

    /**
     * Вычитывает и возвращает новые сообщения (если они есть)
     *
     * @return AMQPEnvelope|array
     * @throws AMQPQueueException
     */
    public function getNextMessage()
    {
        $this->checkConnection();

        // Случайно выбираем очередь из числа подписанных
        $queueKey = array_rand($this->queues);
        $queue = $this->queues[$queueKey];

        $this->currentQueue = $queue;

        try {
            /** @var AMQPEnvelope $message */
            $message = $queue->get(AMQP_NOPARAM);
        } catch (AMQPQueueException $e) {
            if ((int)$e->getCode() === 404) {
                sleep(3);
            }
            throw $e;
        }

        if (is_bool($message)) {
            return $message;
        }

        return $this->adaptMessage($message);
    }

    /**
     * @param array $message
     *
     * @return mixed
     */
    public function ack(array $message)
    {
        return $this->currentQueue->ack($message['deliveryTag'], AMQP_NOPARAM);
    }

    /**
     * @param array $message
     *
     * @return mixed
     */
    public function nack(array $message)
    {
        return $this->currentQueue->nack($message['deliveryTag'], AMQP_NOPARAM);
    }

    public function subscribe($queueName)
    {
        if (empty($this->queues[$queueName])) {
            $this->queues[$queueName] = new AMQPQueue($this->channel);
            $this->queues[$queueName]->setName($queueName);
        }

        return $this->queues[$queueName];
    }

    public function subscribeAll(): void
    {
        foreach ($this->queues as $queueName => $queue) {
            if (empty($queue)) {
                $this->subscribe($queueName);
            }
        }
    }

    public function unsubscribe($queueName): void
    {
        if (empty($this->queues[$queueName])) {
            return;
        }

        unset($this->queues[$queueName]);
    }

    public function unsubscribeAll(): void
    {
        foreach ($this->queues as $queueName => $queue) {
            if (empty($queue)) {
                $this->unsubscribe($queueName);
            }
        }
    }

    /**
     * Отписываемся от всех активных подписок и чистим список очередей
     */
    public function clearSubscribes(): void
    {
        $this->unsubscribeAll();
        $this->queues = null;
    }

    public function __sleep()
    {
        return ['hosts', 'login', 'password', 'settings', 'connection', 'channel', 'queues', 'currentQueue'];
    }

    /**
     * @param AMQPEnvelope $message
     *
     * @return array
     */
    protected function adaptMessage(AMQPEnvelope $message): array
    {
        $headers = $message->getHeaders();
        $headers['destination'] = $this->currentQueue->getName();

        return [
            'body'        => $message->getBody(),
            'headers'     => $headers,
            'deliveryTag' => $message->getDeliveryTag(),
        ];
    }

    /**
     * Определяет, подключен ли объект AMQPConnection к посреднику и есть ли активный канал
     * @return bool
     */
    protected function isConnected(): bool
    {
        return !empty($this->connection) && $this->connection->isConnected()
            && !empty($this->channel) && $this->channel->isConnected();
    }

    /**
     * Проверяет на наличие ошибок
     * @return bool
     */
    protected function hasErrors(): bool
    {
        return false;
    }

    /**
     * Выполняет переподключение
     *
     * @return bool
     * @throws MqAdapterException
     * @throws \AMQPConnectionException
     */
    protected function reconnect(): bool
    {
        if ($this->connection) {
            if ($this->channel) {
                $this->channel->close();
            }

            if (!empty($this->queues)) {
                $queues = array_keys($this->queues);
            }

            $this->connection->reconnect();
            $this->channel = new AMQPChannel($this->connection);
            if ($this->connectType === self::CONNECT_TYPE_PUBLISH) {
                $this->setPublisherConfirms();
            }

            if (isset($queues)) {
                $this->clearSubscribes();
                foreach ($queues as $queue) {
                    $this->subscribe($queue);
                }
            }

            return true;
        }

        return $this->connect();
    }

    /**
     * Включаем механизм подтверждения публикации сообщения
     */
    private function setPublisherConfirms(): void
    {
        $this->channel->setPrefetchCount(1);
        $this->channel->confirmSelect();
        $this->channel->setConfirmCallback(function ($delivery_tag, $multiple) {
            echo 'Message acked: ' . $delivery_tag . '/' . ($multiple ? 'multiple' : 'noMultiple'), PHP_EOL;

            return true;
        }, function ($delivery_tag, $multiple, $requeue) {
            throw new MqAdapterException(
                'Message nacked: ' . $delivery_tag . '/' . ($multiple ? 'multiple' : 'noMultiple') . '/' . $requeue
            );
        });
        $this->channel->setReturnCallback(
            function ($reply_code, $reply_text, $exchange, $routing_key, AMQPBasicProperties $properties, $body) {
                echo 'Message returned: ', $reply_code, '-', $reply_text, ', message body:', $body, PHP_EOL;
            }
        );
    }

    /**
     * @return bool
     * @throws MqAdapterException
     * @throws \AMQPConnectionException
     */
    protected function connect(): bool
    {
        $this->errors = [];

        foreach ($this->hosts as $host) {
            [$host, $port] = explode(':', $host);

            $heartbeat = $this->settings['heartbeat'] ?? null;

            $credentials = [
                'host'            => $host,
                'port'            => $port,
                'vhost'           => $this->settings['vhost'],
                'login'           => $this->login,
                'password'        => $this->password,
                'connect_timeout' => $this->settings['connect_timeout'] ?? 0,
            ];

            if (!empty($this->settings['cacert'])) {
                $credentials['cacert'] = $this->settings['cacert'];
                $credentials['verify'] = $this->settings['verify'];
            }

            if ($heartbeat !== null) {
                $credentials['heartbeat'] = $heartbeat;
            }

            $link = new AMQPConnection($credentials);

            try {
                $link->connect();
            } catch (\Exception $e) {
                $this->errors[] = '[' . $host . ']: ' . $e->getMessage();
            }

            if ($link->isConnected()) {
                $this->connection = $link;

                $this->channel = new AMQPChannel($link);

                if ($this->connectType === self::CONNECT_TYPE_PUBLISH) {
                    $this->setPublisherConfirms();
                }

                return true;
            }
        }

        if ($this->errors) {
            $errors = implode('; ', $this->errors);
            throw new MqAdapterException('Could`nt connect to Broker by provided hosts: ' . $errors);
        }

        return false;
    }
}