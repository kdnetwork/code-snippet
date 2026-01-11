<?php

namespace KDNETWORK\PDO;

use PDOStatement;

class PDOConn
{
    public $DBMode = "";

    public $conn = null;

    private $counter = 0;

    public function __construct()
    {
        if (!class_exists("pdo")) {
            throw new \Exception('Not support PDO');
        }
    }

    public function Close()
    {
        $this->conn = null;
    }

    // dsn builder // https://github.com/halojoy/PHP-PDO-DSN-Creator/blob/master/install/class/DSNCreator.php
    public function ConnectToMysql(string $username = '', string $password = '', string $host = '127.0.0.1', int $port = 3306, string $dbname = '', string $tls_option = '', $long_conn = false)
    {
        $this->DBMode = "mysql";
        $options = [
            \PDO::ATTR_PERSISTENT => $long_conn,
            \PDO::ATTR_EMULATE_PREPARES => false,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
        ];

        if ($tls_option) {
            $options[\PDO::MYSQL_ATTR_SSL_CA] = $tls_option;
            $options[\PDO::MYSQL_ATTR_SSL_VERIFY_SERVER_CERT] = true;
        }

        $dsn = "mysql:host=$host;port=$port;charset=utf8mb4";

        if ($dbname) {
            $dsn .= ";dbname=$dbname";
        }

        try {
            $this->conn = new \PDO($dsn, $username, $password, $options);
        } catch (\PDOException $e) {
            throw new $e;
        }
        return $this->conn;
    }
    public function ConnectToPostgreSQL(string $username = '', string $password = '', string $host = '127.0.0.1', int $port = 5432, string $dbname = '', string $tls_option = '', $long_conn = false)
    {
        $this->DBMode = "postgresql";

        if (!$dbname) {
            $dbname = 'postgres';
        }

        $dsn = "postgresql:host=$host;port=$port;dbname=$dbname";

        if ($tls_option) {
            $lowerTLSOption = strtolower($tls_option);
            if (\in_array($lowerTLSOption, ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"])) {
                $dsn .= "sslmode=$lowerTLSOption";
            } else {
                $dsn .= "sslmode=verify-full;sslrootcert=$tls_option";
            }
        }

        try {
            $this->conn = new \PDO($dsn, $username, $password, [
                \PDO::ATTR_PERSISTENT => $long_conn,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            ]);
        } catch (\PDOException $e) {
            throw new $e;
        }
        return $this->conn;
    }
    public function ConnectToSqlite(string $dbpath = '', $long_conn = false)
    {
        $this->DBMode = "sqlite";
        try {
            $this->conn = new \PDO("sqlite:$dbpath", null, null, [
                \PDO::ATTR_PERSISTENT => $long_conn,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            ]);
        } catch (\PDOException $e) {
            throw new $e;
        }
        return $this->conn;
    }
    public function SetSQLiteWAL()
    {
        if ($this->DBMode !== "sqlite" || $this->conn === null) {
            return;
        }

        $this->conn->exec("PRAGMA journal_mode = WAL;PRAGMA busy_timeout = 5000;PRAGMA synchronous = NORMAL;PRAGMA cache_size = 100000;PRAGMA foreign_keys = true;PRAGMA temp_store = memory;");
    }

    public function GetVersion(): string
    {
        return $this->conn->getAttribute(\PDO::ATTR_SERVER_VERSION);
    }

    public function In(array $params): array
    {
        if (\count($params) === 0) {
            return ["", []];
        }

        $ph = [];
        $new_params = [];
        foreach ($params as $param) {
            $key = ":in{$this->counter}";
            $this->counter++;
            $ph[] = $key;
            $new_params[$key] = $param;
        }

        return [implode(',', $ph), $new_params];
    }

    public function Query(string $query, mixed ...$params): bool|PDOStatement
    {
        $stmt = $this->conn->prepare($query);
        if (\count($params) === 1 && \is_array($params[0])) {
            $params = $params[0];
        }
        $stmt->execute($params);

        return $stmt;
    }
}
