<?php

namespace Tochka\MQAdapter\Protocols;

/**
 * Class Stomp
 * @package Tochka\MQAdapter\Protocols
 */
class Stomp
{
    const DEFAULT_STOMP_VERSION = '1.2';
    /*** @var resource */
    protected $stomp;

    /** @var array */
    protected $hosts;
    /** @var string */
    protected $login;
    /** @var string */
    protected $password;
    /** @var array */
    protected $headers = [];

    /** @var string[] */
    protected $errors = [];

    /** @var array */
    protected $queues = [];

    /**
     * Примеры $connectionString:
     * - Use only one broker uri: tcp://localhost:61614
     * - Use failover in given order: failover://(tcp://localhost:61614,ssl://localhost:61612)
     *
     * @param string $connectionString
     * @param string $login
     * @param string $password
     * @param array  $headers
     *
     * @throws \Exception
     */
    public function __construct($connectionString, $login, $password, array $headers = [])
    {
        $this->hosts = $this->parseConnectionString($connectionString);
        $this->login = $login;
        $this->password = $password;
        $this->headers = array_merge(['accept-version' => self::DEFAULT_STOMP_VERSION], $headers);

        $this->checkConnection();
    }

    /**
     * При уничтожении объекта - отключаемся
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Отправляет сообщение
     * @param string $destination
     * @param string $message
     * @param array  $headers
     *
     * @throws \Exception
     */
    public function send($destination, $message, $headers = [])
    {
        $this->checkConnection();

        $headers = $headers['headers'];

        $receiptId = uniqid('message', true);

        $headers += ['receipt' => $receiptId];

        if (!stomp_send($this->stomp, $destination, $message, $headers)) {
            $error = stomp_error($this->stomp);

            throw new \RuntimeException($error);
        }
    }

    /**
     * Вычитывает и возвращает новые сообщения (если они есть)
     * @return array|null
     * @throws StompAdapterException
     */
    public function getNextMessage()
    {
        $this->checkConnection();

        if (!stomp_has_frame($this->stomp)) {
            return null;
        }

        return stomp_read_frame($this->stomp);
    }

    /**
     * Подтверждает обработку сообщения
     * @param $frame
     *
     * @return bool
     */
    public function ack($frame)
    {
        $id = !empty($frame['headers']['ack']) ? $frame['headers']['ack'] : $frame;
        return stomp_ack($this->stomp, $id, ['id' => $id]);
    }

    /**
     * Отклоняет обработку сообщения
     * @param $frame
     *
     * @return mixed
     */
    public function nack($frame)
    {
        $id = !empty($frame['headers']['ack']) ? $frame['headers']['ack'] : $frame;
        return stomp_nack($this->stomp, $id, ['id' => $id]);
    }

    /**
     * Проверяет, есть ли в данный момент соединение
     * @throws StompAdapterException
     */
    public function checkConnection()
    {
        if (!$this->isStompResource() || $this->hasErrors()) {
            $this->reconnect();
        }
    }

    /**
     * Выполняет подключение
     * @throws StompAdapterException
     */
    public function connect()
    {
        $this->errors = [];
        $link = null;

        foreach ($this->hosts as $host) {
            try {
                $link = stomp_connect($host, $this->login, $this->password, $this->headers);
                if (!$link) {
                    $this->errors[] = '[' . $host . ']: ' . stomp_connect_error();
                }
            } catch (\Exception $e) {
                $this->errors[] = '[' . $host . ']: ' . stomp_connect_error();
            }

            if ($link) {
                break;
            }
        }

        if (!$link) {
            if (empty($this->errors)) {
                $errors = stomp_connect_error();
            } else {
                $errors = implode('; ', $this->errors);
            }
            throw new StompAdapterException('Could`nt connect to Brocker by provided hosts: ' . $errors);
        }

        $this->stomp = $link;
    }

    /**
     * Выполняет переподключение
     * @throws StompAdapterException
     */
    public function reconnect()
    {
        $this->disconnect();
        $this->connect();
        $this->subscribeAll();
    }

    /**
     * Выполняет отключение
     */
    public function disconnect()
    {
        if ($this->isStompResource()) {
            $this->unsubscribeAll();
            stomp_close($this->stomp);
        }
    }

    /**
     * Подписываемся на все сохраненные подписки
     */
    public function subscribeAll()
    {
        foreach ($this->queues as $queue => $status) {
            if (empty($status)) {
                $this->subscribe($queue);
            }
        }
    }

    /**
     * Отписываемся от всех активных подписок
     */
    public function unsubscribeAll()
    {
        foreach ($this->queues as $queue => $status) {
            if (!empty($status)) {
                $this->unsubscribe($queue);
            }
        }
    }

    /**
     * Отписываемся от всех активных подписок и чистим список очередей
     */
    public function clearSubscribes()
    {
        $this->unsubscribeAll();
        $this->queues = [];
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
        if (empty($this->queues[$queue])) {
            $this->queues[$queue] = uniqid('client', true);

            stomp_subscribe($this->stomp, $queue, [
                'id' => $this->queues[$queue],
                'ack' => 'client-individual'
            ]);
        }

        return $this->queues[$queue];
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
        if (empty($this->queues[$queue])) {
            return;
        }

        stomp_unsubscribe($this->stomp, $queue, [
            'id' => $this->queues[$queue]
        ]);
        $this->queues[$queue] = false;
    }

    /**
     * Проверяет, что у нас есть активный ресурс подключения
     * @return bool
     */
    protected function isStompResource()
    {
        return !empty($this->stomp) && is_resource($this->stomp);
    }

    /**
     * Проверяет на наличие ошибок
     * @return bool
     */
    protected function hasErrors()
    {
        return stomp_error($this->stomp);
    }

    /**
     * Сериализуем только важные данные
     * @return array
     */
    public function __sleep()
    {
        return ['hosts', 'login', 'password', 'headers', 'errors', 'queues'];
    }
}
