<?php


namespace Tochka\MQAdapter;

use Tochka\MQAdapter\Exceptions\MqAdapterException;

/**
 * Class MqAdapter
 * @package Tochka\MQAdapter
 */
class MqAdapter
{

    /** @var array */
    protected $hosts;
    /** @var string */
    protected $login;
    /** @var string */
    protected $password;
    /** @var array */
    protected $settings = [];
    /** @var string[] */
    protected $errors = [];
    /** @var array */
    protected $queues = [];
    /** @var string */
    protected $protocol;
    /** @var \stdClass */
    protected $adapter;

    /**
     * MqAdapter constructor.
     *
     * @param string $protocol
     * @param string $dsn
     * @param string $login
     * @param string $password
     * @param array  $settings
     *
     * @throws MqAdapterException
     */
    public function __construct(string $protocol, string $dsn, string $login, string $password, array $settings)
    {
        $this->protocol = $protocol;
        $this->hosts = $this->parseConnectionString($dsn);
        $this->login = $login;
        $this->password = $password;
        $this->settings = $settings;

        $className = 'Tochka\\MQAdapter\\Protocols\\' . ucfirst($protocol);
        $this->adapter = new $className($this->hosts, $login, $password, $settings);

        $this->adapter->checkConnection();
    }

    /**
     * При уничтожении объекта - отключаемся
     */
    public function __destruct()
    {
        $this->adapter->disconnect();
    }

    /**
     * Отправляет сообщение
     *
     * @param string $destination
     * @param string $message
     * @param array  $settings
     *
     * @return
     * @throws \Exception
     */
    public function send(string $destination, string $message, array $settings = [])
    {
        return $this->adapter->send($destination, $message, $settings);
    }

    /**
     * Вычитывает и возвращает новые сообщения (если они есть)
     * @return array|null
     */
    public function getNextMessage()
    {
        return $this->adapter->getNextMessage();
    }

    /**
     * Подтверждает обработку сообщения
     *
     * @param $message
     *
     * @return bool
     */
    public function ack($message)
    {
        return $this->adapter->ack($message);
    }

    /**
     * Отклоняет обработку сообщения
     *
     * @param $message
     *
     * @return bool
     */
    public function nack($message)
    {
        return $this->adapter->nack($message);
    }

    /**
     * Подписываемся на все сохраненные подписки
     */
    public function subscribeAll()
    {
        $this->adapter->subscribeAll();
    }

    /**
     * Отписываемся от всех активных подписок
     */
    public function unsubscribeAll()
    {
        $this->adapter->unsubscribeAll();
    }

    /**
     * Отписываемся от всех активных подписок и чистим список очередей
     */
    public function clearSubscribes()
    {
        $this->adapter->clearSubscribes();
    }

    /**
     * Подписываемся на очередь
     *
     * @param string $queue
     *
     * @return mixed
     */
    public function subscribe($queue)
    {
        return $this->adapter->subscribe($queue);
    }

    /**
     * Отписываемся от очереди
     *
     * @param string $queue
     *
     * @return void
     */
    public function unsubscribe($queue)
    {
        return $this->adapter->unsubscribe($queue);
    }
    
    /**
     * Сериализуем только важные данные
     * @return array
     */
    public function __sleep()
    {
        return ['hosts', 'login', 'password', 'settings', 'errors', 'queues', 'protocol', 'adapter'];
    }

    /**
     * Парсит строку подключения
     *
     * @param string $connectionString
     *
     * @return array
     * @throws MqAdapterException
     */
    protected function parseConnectionString($connectionString): array
    {
        $hosts = [];

        $pattern = "|^(([a-zA-Z0-9]+)://)+\(*([a-zA-Z0-9\.:/i,-_]+)\)*$|i";
        if (preg_match($pattern, $connectionString, $matches)) {

            [, , $scheme, $hostsPart] = $matches;

            if ($scheme !== 'failover') {
                $hosts[] = $hostsPart;
            } else {
                foreach (explode(',', $hostsPart) as $url) {
                    $hosts[] = $url;
                }
            }
        }

        if (empty($hosts)) {
            throw new MqAdapterException('Bad Broker URL ' . $connectionString . 'Check used scheme!');
        }

        return $hosts;
    }
}
