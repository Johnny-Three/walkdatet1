package main

import (
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"os"
	"time"
	. "wbproject/walkdatet1/client"
	. "wbproject/walkdatet1/dbhelper"
	. "wbproject/walkdatet1/envbuild"
	. "wbproject/walkdatet1/logs"
	. "wbproject/walkdatet1/process"
	. "wbproject/walkdatet1/structure"
)

var err error
var consumer *nsq.Consumer
var version string = "1.0.0PR9"

var def = 100

func main() {

	args := os.Args

	if len(args) == 2 && (args[1] == "-v") {

		fmt.Println("看好了兄弟，现在的版本是【", version, "】，可别弄错了")
		os.Exit(0)
	}

	db, nsqadress, err := EnvBuild()
	if err != nil {
		panic(err.Error())
	}

	var init bool
	if len(args) == 2 && (args[1] == "-i") {

		init = true

		start := time.Now()
		//环境初始化，半年内的数据进行初始化
		users, err := SelectAllUsers(db)
		fmt.Println("load db game over the len of users is", len(users))
		elapsed := time.Since(start)
		fmt.Println("Load db person query total time:", elapsed)

		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}

		Sync(users, db, def)
		os.Exit(1)
	}

	flag.Parse()

	if !init {

		//对接NSQ，消费上传消息
		consumer, err = NewConsummer("base_data_upload", "walkdatet1")
		if err != nil {
			panic(err)
		}

		//Consumer运行，消费消息..
		go func(consumer *nsq.Consumer) {

			err := ConsumerRun(consumer, "base_data_upload", nsqadress)
			if err != nil {
				panic(err)
			}
		}(consumer)

		//正常流程
		for {
			select {

			case m := <-User_walk_data_chan:
				fmt.Println("get msg", m)
				Logger.Info("get msg", m)
				DealNsqMsq(db, &m)

			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}
