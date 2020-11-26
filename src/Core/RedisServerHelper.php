<?php declare(strict_types=1);
/**
 * Created by PhpStorm
 * User: Hety
 * Date: 2020/11/26 11:54
 */

namespace GameWiki\Core;
/**
 * Class RedisServerHelper
 * @package GameWiki\Core
 */
class RedisServerHelper {

    /**
     * @var int
     */
    public const TTL_YEAR = 31536000; // 365 * 24 * 3600

    /**
     * Max time expected to pass between delete() and DB commit finishing
     */
    public const MAX_COMMIT_DELAY = 3;

    /**
     * Max replication+snapshot lag before applying TTL_LAGGED or disallowing set()
     */
    public const MAX_READ_LAG = 7;

    /**
     * Seconds to tombstone keys on delete()
     */
    public const HOLDOFF_TTL = 11; // MAX_COMMIT_DELAY + MAX_READ_LAG + 1

    /**
     * Seconds to keep dependency purge keys around
     */
    public const CHECK_KEY_TTL = self::TTL_YEAR;

    /**
     * Seconds to keep interim value keys for tombstoned keys around
     */
    public const INTERIM_KEY_TTL = 1;

    /**
     * Seconds to keep lock keys around
     */
    public const LOCK_TTL = 10;

    /**
     * Seconds to no-op key set() calls to avoid large blob I/O stampedes
     */
    public const COOLOFF_TTL = 1;

    /**
     * Default remaining TTL at which to consider pre-emptive regeneration
     */
    public const LOW_TTL = 30;

    /**
     * Never consider performing "popularity" refreshes until a key reaches this age
     */
    public const AGE_NEW = 60;

    /**The time length of the "popularity" refresh window for hot keys
     */
    public const HOT_TTR = 900;
    /**
     * Hits/second for a refresh to be expected within the "popularity" window
     */
    public const HIT_RATE_HIGH = 1;
    /**
     * Seconds to ramp up to the "popularity" refresh chance after a key is no longer new
     */
    public const RAMPUP_TTL = 30;

    /**
     * Idiom for getWithSetCallback() callbacks to avoid calling set()
     */
    public const TTL_UNCACHEABLE = -1;
    /**
     * Idiom for getWithSetCallback() callbacks to 'lockTSE' logic
     */
    public const TSE_NONE = -1;
    /**
     * Max TTL to store keys when a data sourced is lagged
     */
    public const TTL_LAGGED = 30;
    /**
     * Idiom for delete() for "no hold-off"
     */
    public const HOLDOFF_NONE = 0;
    /**
     * Idiom for set()/getWithSetCallback() for "do not augment the storage medium TTL"
     */
    public const STALE_TTL_NONE = 0;
    /**
     * Idiom for set()/getWithSetCallback() for "no post-expired grace period"
     */
    public const GRACE_TTL_NONE = 0;

    /**
     * Idiom for getWithSetCallback() for "no minimum required as-of timestamp"
     */
    public const MIN_TIMESTAMP_NONE = 0.0;

    /**
     * Tiny negative float to use when CTL comes up >= 0 due to clock skew
     */
    public const TINY_NEGATIVE = -0.000001;
    /**
     * Tiny positive float to use when using "minTime" to assert an inequality
     */
    public const TINY_POSTIVE = 0.000001;

    /**
     * Milliseconds of delay after get() where set() storms are a consideration with 'lockTSE'
     */
    public const SET_DELAY_HIGH_MS = 50;
    /**
     * Min millisecond set() backoff for keys in hold-off (far less than INTERIM_KEY_TTL)
     */
    public const RECENT_SET_LOW_MS = 50;
    /**
     * Max millisecond set() backoff for keys in hold-off (far less than INTERIM_KEY_TTL)
     */
    public const RECENT_SET_HIGH_MS = 100;

    /**
     * Parameter to get()/getMulti() to return extra information by reference
     */
    public const PASS_BY_REF = -1;

    /**
     * Cache format version number
     */
    public const VERSION = 1;

    /**
     * value wrapped
     * @var int
     */
    public const FLD_VERSION = 0; // key to cache version number
    public const FLD_VALUE = 1; // key to the cached value
    public const FLD_TTL = 2; // key to the original TTL
    public const FLD_TIME = 3; // key to the cache time
    public const FLD_FLAGS = 4; // key to the flags bitfield (reserved number)
    public const FLD_HOLDOFF = 5; // key to any hold-off TTL

    /**
     * key prefix
     * @var string
     */
    public const VALUE_KEY_PREFIX = 'WANCache:v:';
    public const INTERIM_KEY_PREFIX = 'WANCache:i:';
    public const TIME_KEY_PREFIX = 'WANCache:t:';
    public const MUTEX_KEY_PREFIX = 'WANCache:m:';
    public const COOLOFF_KEY_PREFIX = 'WANCache:c:';
    public const PURGE_VAL_PREFIX = 'PURGED:';

    /**
     * @var string
     */
    public const VFLD_DATA = 'WOC:d'; // key to the value of versioned data
    public const VFLD_VERSION = 'WOC:v'; // key to the version of the value present
    public const PC_PRIMARY = 'primary:1000'; // process cache name and max key count


    /**
     * @var array List of server names
     */
    protected $servers;

    /**
     * @var array Map of (tag => server name)
     */
    protected $serverTagMap;

    /**
     * @var bool
     */
    protected $automaticFailover;

    /**
     * @var float Unix timestamp of the oldest possible valid values
     */
    protected $epoch;


    /**
     * RedisServerHelper constructor.
     * @param array $params
     */
    public function __construct(array $params)
    {
        $this->servers = $params['servers'];
        foreach ($this->servers as $key => $server) {
            $this->serverTagMap[is_int($key) ? $server : $key] = $server;
        }
        $this->automaticFailover = $params['automaticFailover'] ?? true;
        $this->epoch = $params['epoch'] ?? 1.0;
    }

    /**
     * @param string $key
     * @return array
     */
    public function getServer(string $key): array
    {
        $candidates = array_keys($this->serverTagMap);
        if (count($this->servers) > 1) {
            self::consistentHashSort($candidates, $key, '/');
            if (!$this->automaticFailover) {
                $candidates = array_slice($candidates, 0, 1);
            }
        }
        $server = [];
        while (($tag = array_shift($candidates)) !== null) {
            $server = $this->serverTagMap[$tag] ?? [];
            if (!empty($server)) {
                break;
            }
        }
        return $server;
    }

    /**
     * Sort the given array in a pseudo-random order which depends only on the
     * given key and each element value in $array. This is typically used for load
     * balancing between servers each with a local cache.
     *
     * Keys are preserved. The input array is modified in place.
     *
     * Note: Benchmarking on PHP 5.3 and 5.4 indicates that for small
     * strings, md5() is only 10% slower than hash('joaat',...) etc.,
     * since the function call overhead dominates. So there's not much
     * justification for breaking compatibility with installations
     * compiled with ./configure --disable-hash.
     *
     * @param array &$array Array to sort
     * @param string $key
     * @param string $separator A separator used to delimit the array elements and the
     *     key. This can be chosen to provide backwards compatibility with
     *     various consistent hash implementations that existed before this
     *     function was introduced.
     */
    public static function consistentHashSort(&$array, string $key, $separator = "\000")
    {
        $hashes = [];
        foreach ($array as $elt) {
            $hashes[$elt] = md5($elt . $separator . $key);
        }
        uasort($array, function ($a, $b) use ($hashes) {
            return strcmp($hashes[$a], $hashes[$b]);
        });
    }


    /**
     * @param $value
     * @param int $ttl
     * @param null $now
     * @return array
     */
    public function wrap($value, $ttl = 3600, $now = null)
    {
        return [
            self::FLD_VERSION => self::VERSION,
            self::FLD_VALUE => $value,
            self::FLD_TTL => $ttl,
            self::FLD_TIME => $now ?? time()
        ];
    }


    /**
     * @param $wrapped
     * @return mixed
     */
    public function getUnWrapValue($wrapped)
    {
        return $this->unwrap($wrapped, time())[0];
    }

    /**
     * Do not use this method outside WANObjectCache
     *
     * The cached value will be false if absent/tombstoned/malformed
     *
     * @param array|string|bool $wrapped
     * @param float $now Unix Current timestamp (preferrably pre-query)
     * @return array (cached value or false, current TTL, value timestamp, tombstone timestamp)
     */
    public function unwrap($wrapped, $now)
    {
        // Check if the value is a tombstone
        $purge = $this->parsePurgeValue($wrapped);
        if ($purge !== false) {
            // Purged values should always have a negative current $ttl
            $curTTL = min($purge[self::FLD_TIME] - $now, self::TINY_NEGATIVE);
            return [false, $curTTL, null, $purge[self::FLD_TIME]];
        }

        if (!is_array($wrapped) // not found
            || !isset($wrapped[self::FLD_VERSION]) // wrong format
            || $wrapped[self::FLD_VERSION] !== self::VERSION // wrong version
        ) {
            return [false, null, null, null];
        }

        if ($wrapped[self::FLD_TTL] > 0) {
            // Get the approximate time left on the key
            $age = $now - $wrapped[self::FLD_TIME];
            $curTTL = max($wrapped[self::FLD_TTL] - $age, 0.0);
        } else {
            // Key had no TTL, so the time left is unbounded
            $curTTL = INF;
        }

        if ($wrapped[self::FLD_TIME] < $this->epoch) {
            // Values this old are ignored
            return [false, null, null, null];
        }

        return [$wrapped[self::FLD_VALUE], $curTTL, $wrapped[self::FLD_TIME], null];
    }

    /**
     * @param string|array|bool $value Possible string of the form "PURGED:<timestamp>:<holdoff>"
     * @return array|bool Array containing a UNIX timestamp (float) and holdoff period (integer),
     *  or false if value isn't a valid purge value
     */
    protected function parsePurgeValue($value)
    {
        if (!is_string($value)) {
            return false;
        }

        $segments = explode(':', $value, 3);
        if (!isset($segments[0]) || !isset($segments[1]) || "{$segments[0]}:" !== self::PURGE_VAL_PREFIX) {
            return false;
        }

        if (!isset($segments[2])) {
            // Back-compat with old purge values without holdoff
            $segments[2] = self::HOLDOFF_TTL;
        }

        if ($segments[1] < $this->epoch) {
            // Values this old are ignored
            return false;
        }

        return [
            self::FLD_TIME => (float)$segments[1],
            self::FLD_HOLDOFF => (int)$segments[2],
        ];
    }

}