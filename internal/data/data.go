package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/uptrace/bun/extra/bunotel"
)

const ConnTimeout = time.Second * 6

func NewDSN(user, pwd, host, db string, port uint) string {
	connStr := "%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local" +
		"&interpolateParams=true"
	return fmt.Sprintf(connStr, user, pwd, host, port, db)
}

type Conf struct {
	DSN string
}

func NewPostgres[T ~*bun.DB](c *Conf, l *log.Entry) (res T, cleanup func(), err error) {
	l = log.WithFields(log.Fields{"func": "NewPostgres"})
	// The config has defaults for timeouts,
	// so it's not necessary to specify them again:
	//	 - DialTimeout: 5 * time.Second
	//   - ReadTimeout:  10 * time.Second
	//   - WriteTimeout: 5 * time.Second
	connector := pgdriver.NewConnector(pgdriver.WithDSN(c.DSN))
	sqldb := sql.OpenDB(connector)
	db := bun.NewDB(sqldb, pgdialect.New(), bun.WithDiscardUnknownColumns())
	db.AddQueryHook(bundebug.NewQueryHook())
	db.AddQueryHook(bunotel.NewQueryHook(
		bunotel.WithDBName(connector.Config().Database),
	))
	err = db.Ping()
	cleanup = func() {
		lh := log.NewHelper(log.With(l, "do", "cleanup"))
		lh.Info("starting")
		if err := db.Close(); err != nil {
			lh.Errorw("err", err)
		}
	}
	res = T(db)
	return
}

type RedisConf struct {
	DSN string
}

// NewRedis returns a new [redis.Client] according to the given URL.
// Default configs:
//   - DialTimeout = 5 * time.Second
//   - ReadTimeout = 3 * time.Second
//   - WriteTimeout = ReadTimeout
func NewRedis(cf *RedisConf, l log.Logger) (rdb *redis.Client, cleanup func(), err error) {
	opt, err := redis.ParseURL(cf.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("parse url: %w", err)
	}

	h := log.NewHelper(log.With(l, "func", "NewRDB"))
	rdb = redis.NewClient(opt)
	rdb.AddHook(redisotel.NewTracingHook())
	ctx, cancel := context.WithTimeout(context.Background(), opt.DialTimeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, nil, fmt.Errorf("ping: %w", err)
	}

	cleanup = func() {
		if err := rdb.Close(); err != nil {
			h.Error(err)
		}
	}
	return
}

func CheckRedis(rdb *redis.Client) func(context.Context) error {
	return func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}
}
