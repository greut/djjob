<?php

namespace djjob;

use Exception;

class DJJob extends DJBase {
    protected $worker_name;
    protected $job_id;
    protected $max_attempts;

    public function __construct($worker_name, $job_id, $options = array()) {
        $options = array_merge(array(
            "max_attempts" => 5,
        ), $options);
        $this->worker_name = $worker_name;
        $this->job_id = $job_id;
        $this->max_attempts = $options["max_attempts"];
    }

    public function run() {
        # pull the handler from the db
        $handler = $this->getHandler();
        if (!is_object($handler)) {
            $this->log("[JOB] bad handler for job::{$this->job_id}", self::ERROR);
            $this->finishWithError("bad handler for job::{$this->job_id}");
            return false;
        }

        # run the handler
        try {
            $handler->perform();

            # cleanup
            $this->finish();
            return true;

        } catch (DJRetryException $e) {
            # attempts hasn't been incremented yet.
            $attempts = $this->getAttempts()+1;

            $msg = "Caught DJRetryException \"{$e->getMessage()}\" on attempt $attempts/{$this->max_attempts}.";

            if($attempts == $this->max_attempts) {
                $this->log("[JOB] job::{$this->job_id} $msg Giving up.");
                $this->finishWithError($msg);
            } else {
                $this->log("[JOB] job::{$this->job_id} $msg Try again in {$e->getDelay()} seconds.", self::WARN);
                $this->retryLater($e->getDelay());
            }
            return false;

        } catch (Exception $e) {

            $this->finishWithError($e->getMessage());
            return false;

        }
    }

    public function acquireLock() {
        $this->log("[JOB] attempting to acquire lock for job::{$this->job_id} on {$this->worker_name}", self::INFO);

        $table = self::$options["mysql_table"];
        $lock = $this->runUpdate(
            "UPDATE `{$table}`
             SET    locked_at = NOW(), locked_by = ?
             WHERE  id = ? AND (locked_at IS NULL OR locked_by = ?) AND failed_at IS NULL",
            array(
                $this->worker_name,
                $this->job_id,
                $this->worker_name
            )
        );

        if (!$lock) {
            $this->log("[JOB] failed to acquire lock for job::{$this->job_id}", self::INFO);
            return false;
        }

        return true;
    }

    public function releaseLock() {
        $table = self::$options["mysql_table"];
        $this->runUpdate(
            "UPDATE `{$table}`
             SET locked_at = NULL, locked_by = NULL
             WHERE id = ?",
            array($this->job_id)
        );
    }

    public function finish() {
        $table = self::$options["mysql_table"];
        $this->runUpdate(
            "DELETE FROM `{$table}` WHERE id = ?",
            array($this->job_id)
        );
        $this->log("[JOB] completed job::{$this->job_id}", self::INFO);
    }

    public function finishWithError($error) {
        $table = self::$options["mysql_table"];
        $this->runUpdate(
            "UPDATE `{$table}`
             SET attempts = attempts + 1,
                 failed_at = IF(attempts >= ?, NOW(), NULL),
                 error = IF(attempts >= ?, ?, NULL)
             WHERE id = ?",
            array(
                $this->max_attempts,
                $this->max_attempts,
                $error,
                $this->job_id
            )
        );
        $this->log("[JOB] failure in job::{$this->job_id}", self::ERROR);
        $this->releaseLock();
    }

    public function retryLater($delay) {
        $table = self::$options["mysql_table"];
        $this->runUpdate(
            "UPDATE `{$table}`
             SET run_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                 attempts = attempts + 1
             WHERE id = ?",
            array(
                $delay,
                $this->job_id
            )
        );
        $this->releaseLock();
    }

    public function getHandler() {
        $table = self::$options["mysql_table"];
        $rs = $this->runQuery(
            "SELECT handler FROM `{$table}` WHERE id = ?",
            array($this->job_id)
        );
        foreach ($rs as $r) return unserialize($r["handler"]);
        return false;
    }

    public function getAttempts() {
        $table = self::$options["mysql_table"];
        $rs = $this->runQuery(
            "SELECT attempts FROM `{$table}` WHERE id = ?",
            array($this->job_id)
        );
        foreach ($rs as $r) return $r["attempts"];
        return false;
    }

    public static function enqueue($handler, $queue = "default", $run_at = null) {
        $table = self::$options["mysql_table"];
        $affected = self::runUpdate(
            "INSERT INTO `{$table}` (handler, queue, run_at, created_at)
             VALUES(?, ?, ?, NOW())",
            array(
                serialize($handler),
                (string) $queue,
                $run_at
            )
        );

        if ($affected < 1) {
            self::log("[JOB] failed to enqueue new job", self::ERROR);
            return false;
        }

        return true;
    }

    public static function bulkEnqueue($handlers, $queue = "default", $run_at = null) {
        $table = self::$options["mysql_table"];
        $sql = "INSERT INTO `{$table}` (handler, queue, run_at, created_at) VALUES";
        $sql .= implode(",", array_fill(0, count($handlers), "(?, ?, ?, NOW())"));

        $parameters = array();
        foreach ($handlers as $handler) {
            $parameters []= serialize($handler);
            $parameters []= (string) $queue;
            $parameters []= $run_at;
        }
        $affected = self::runUpdate($sql, $parameters);

        if ($affected < 1) {
            self::log("[JOB] failed to enqueue new jobs", self::ERROR);
            return false;
        }

        if ($affected != count($handlers))
            self::log("[JOB] failed to enqueue some new jobs", self::ERROR);

        return true;
    }

    public static function status($queue = "default") {
        $table = self::$options["mysql_table"];
        $rs = self::runQuery(
            "SELECT COUNT(*) as total, COUNT(failed_at) as failed, COUNT(locked_at) as locked
             FROM `{$table}`
             WHERE queue = ?",
            array($queue)
        );
        $rs = $rs[0];

        $failed = $rs["failed"];
        $locked = $rs["locked"];
        $total  = $rs["total"];
        $outstanding = $total - $locked - $failed;

        return array(
            "outstanding" => $outstanding,
            "locked" => $locked,
            "failed" => $failed,
            "total"  => $total
        );
    }
}

?>