package envbuild

import (
	"database/sql"
	"flag"
	_ "github.com/go-sql-driver/mysql"
	config "github.com/msbranco/goconfig"
)

var db1 *sql.DB
var listening string
var config_file_path string

func init() {
	flag.StringVar(&config_file_path, "c", "./config.ini", "Use -c <filepath>")
}

func GetDB() *sql.DB {

	return db1
}

//EnvBuild需要正确的解析文件并且初始化DB的连接。。
func EnvBuild() (*sql.DB, string, error) {

	//get conf
	cf, err := config.ReadConfigFile(config_file_path)

	if err != nil {
		return nil, "", err
	}

	rdip1, _ := cf.GetString("DBCONN1", "IP")
	rdusr1, _ := cf.GetString("DBCONN1", "USERID")
	rdpwd1, _ := cf.GetString("DBCONN1", "USERPWD")
	rdname1, _ := cf.GetString("DBCONN1", "DBNAME")

	rdip1 = rdusr1 + ":" + rdpwd1 + "@tcp(" + rdip1 + ")/" + rdname1 + "?charset=utf8"

	//open db1
	db1, _ = sql.Open("mysql", rdip1)
	//defer db1.Close()
	db1.SetMaxOpenConns(50)
	db1.SetMaxIdleConns(10)
	db1.Ping()

	nsqip, _ := cf.GetString("CONSUMER", "IP")
	nsqport, _ := cf.GetString("CONSUMER", "PORT")

	return db1, nsqip + ":" + nsqport, nil
}
