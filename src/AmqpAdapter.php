<?php


namespace Tochka\MQAdapter;

use AMQPChannel;
use AMQPConnection;
use AMQPEnvelope;
use AMQPExchange;
use AMQPQueue;
use Tochka\MQAdapter\Exceptions\MqAdapterException;

class AmqpAdapter
{
    private $hosts;
    private $login;
    private $password;
    private $settings;
    private $connection;
    private $channel;
    private $queues;
    private $currentQueue;

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

    public function send(string $destination, string $message, array $settings = []): bool
    {
        $this->checkConnection();

        $exchange = new AMQPExchange($this->channel);
        $exchange->setName($destination);

        return $exchange->publish($message);
    }

    /**
     * Вычитывает и возвращает новые сообщения (если они есть)
     *
     * @return bool|AMQPEnvelope
     */
    public function getNextMessage()
    {
        $this->checkConnection();

        // Случайно выбираем очередь из числа подписанных
        $queueKey = array_rand($this->queues);

        $queue = $this->queues[$queueKey];

        $this->currentQueue = $queue;

        /** @var AMQPEnvelope $message */
        $message = $queue->get(AMQP_NOPARAM);

        return $message;
    }

    /**
     * @param AMQPEnvelope $message
     *
     * @return mixed
     */
    public function ack(AMQPEnvelope $message)
    {
        return $this->currentQueue->ack($message->getDeliveryTag(), AMQP_NOPARAM);
    }

    /**
     * @param AMQPEnvelope $message
     *
     * @return mixed
     */
    public function nack(AMQPEnvelope $message)
    {
        return $this->currentQueue->nack($message->getDeliveryTag(), AMQP_NOPARAM);
    }

    public function subscribe($queueName)
    {
        if (empty($this->queues[$queueName])) {
            $this->queues[$queueName] = new AMQPQueue($this->channel);
            $this->queues[$queueName]->setName($queueName);
        }

        return $this->queues[$queueName];
    }

    public function subscribeAll()
    {
        foreach ($this->queues as $queueName => $queue) {
            if (empty($queue)) {
                $this->subscribe($queueName);
            }
        }
    }

    public function unsubscribe($queue)
    {

    }

    public function unsubscribeAll(): void
    {

    }

    /**
     * Отписываемся от всех активных подписок и чистим список очередей
     */
    public function clearSubscribes(): void
    {
        $this->unsubscribeAll();
        $this->queues = [];
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
     */
    protected function reconnect(): bool
    {
        if ($this->connection) {

            if ($this->channel) {
                $this->channel->close();
            }

            $this->connection->reconnect();
            $this->channel = new AMQPChannel($this->connection);
        }

        return $this->connect();
    }

    /**
     * @return bool
     * @throws MqAdapterException
     * @throws \AMQPConnectionException
     */
    protected function connect(): bool
    {

        $this->errors = [];
        $link = null;

        foreach ($this->hosts as $host) {

            [$host, $port] = explode(':', $host);

            $credentials = [
                'host'     => $host,
                'port'     => $port,
                'vhost'    => $this->settings['vhost'],
                'login'    => $this->login,
                'password' => $this->password,
            ];

            $link = new AMQPConnection($credentials);

            try {
                $link->connect();
            } catch (\Exception $e) {
                $this->errors[] = '[' . $host . ']: ' . $e->getMessage();
            }

            if ($link->isConnected()) {
                $this->connection = $link;

                $this->channel = new AMQPChannel($link);

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