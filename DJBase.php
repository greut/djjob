<?php

namespace djjob;

use PDO;
use Exception;

class DJBase {

    // error severity levels
    const CRITICAL = 4;
    const    ERROR = 3;
    const     WARN = 2;
    const     INFO = 1;
    const    DEBUG = 0;

    private static $log_level = self::DEBUG;

    private static $db = null;

    private static $dsn = "";
    private static $options = array(
      "mysql_user" => null,
      "mysql_pass" => null,
    );

    // use either `configure` or `setConnection`, depending on if
    // you already have a PDO object you can re-use
    public static function configure($dsn, $options = array()) {
        self::$dsn = $dsn;
        self::$options = array_merge(self::$options, $options);
    }

    public static function setLogLevel($const) {
        self::$log_level = $const;
    }

    public static function setConnection(PDO $db) {
        self::$db = $db;
    }

    protected static function getConnection() {
        if (self::$db === null) {
            if (!self::$dsn) {
                throw new DJException("Please tell DJJob how to connect to your database by calling DJJob::configure(\$dsn, [\$options = array()]) or re-using an existing PDO connection by calling DJJob::setConnection(\$pdoObject). If you're using MySQL you'll need to pass the db credentials as separate 'mysql_user' and 'mysql_pass' options. This is a PDO limitation, see [http://stackoverflow.com/questions/237367/why-is-php-pdo-dsn-a-different-format-for-mysql-versus-postgresql] for an explanation.");
            }
            try {
                // http://stackoverflow.com/questions/237367/why-is-php-pdo-dsn-a-different-format-for-mysql-versus-postgresql
                if (self::$options["mysql_user"] !== null) {
                    self::$db = new PDO(self::$dsn, self::$options["mysql_user"], self::$options["mysql_pass"]);
                } else {
                    self::$db = new PDO(self::$dsn);
                }
                self::$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            } catch (PDOException $e) {
                throw new Exception("DJJob couldn't connect to the database. PDO said [{$e->getMessage()}]");
            }
        }
        return self::$db;
    }

    public static function runQuery($sql, $params = array()) {
        $stmt = self::getConnection()->prepare($sql);
        $stmt->execute($params);

        $ret = array();
        if ($stmt->rowCount()) {
            // calling fetchAll on a result set with no rows throws a
            // "general error" exception
            foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $r) $ret []= $r;
        }

        $stmt->closeCursor();
        return $ret;
    }

    public static function runUpdate($sql, $params = array()) {
        $stmt = self::getConnection()->prepare($sql);
        $stmt->execute($params);
        return $stmt->rowCount();
    }

    protected static function log($mesg, $severity=self::CRITICAL) {
        if ($severity >= self::$log_level) {
            printf("[%s] %s\n", date('Y-m-d H:i:s'), $mesg);
        }
    }
}

?>
