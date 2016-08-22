package process

import (
	"fmt"
	//"strings"
	"database/sql"
	"sync"
	"time"
	//"wbproject/walkdatet1/client"
	. "wbproject/walkdatet1/dbhelper"
	. "wbproject/walkdatet1/logs"
	. "wbproject/walkdatet1/structure"
)

var wg sync.WaitGroup

func CheckError(err error) {
	if err != nil {
		fmt.Println(err)
		Logger.Critical(err)
	}
}

func DealNsqMsq(db *sql.DB, t *User_walkdays_struct) error {

	user := UserDayData{}
	user.MapHourData = make(map[int64]HourData)

	for _, v := range t.Walkdays {

		//如果这两个值为-1，说明这两个字段需要重新计算;如果不为-1，则字段无须重新计算
		if v.Faststepnum == -1 && v.Effecitvestepnum == -1 {
			r, err := AssignUserHourDataNsq1(db, &user, t)
			CheckError(err)
			if err != nil {
				continue
			}

			if r == 1 {
				err := InsertT1N1(db, &user)
				CheckError(err)
			} else if r == 2 {
				err := InsertT1N2(db, &user)
				CheckError(err)
			} else if r == 3 {
				err := InsertT1N3(db, &user)
				CheckError(err)
			}

		} else if v.Faststepnum >= 0 && v.Effecitvestepnum >= 0 {

			r, err := AssignUserHourDataNsq2(db, &user, t)
			CheckError(err)
			if err != nil {
				continue
			}
			if r == 1 {
				err := InsertT1N1(db, &user)
				CheckError(err)
			} else if r == 2 {
				err := InsertT1N2(db, &user)
				CheckError(err)
			} else if r == 3 {
				err := InsertT1N3(db, &user)
				CheckError(err)
			}

		} else {
			//数据格式不正确
			Logger.Critical("error msg format :", t)
		}
	}

	return nil
}

//需要初始化的人群，开始并行初始化，每次def并发量
func Sync(uids []*UserDayData, db *sql.DB, def int) {

	stepth := len(uids) / def
	//fmt.Println("stepth is: ", stepth)

	for i := 0; i < stepth; i++ {

		time.Sleep(1 * time.Millisecond)

		for j := i * def; j < (i+1)*def; j++ {

			wg.Add(1)

			go func(j int) {
				defer wg.Done()
				//todo .. 处理每个用户，从小米获取信息，处理信息并入库
				fmt.Println("hi,Sync is running in batch ")
				err := AssignUserHourData(db, uids[j])
				CheckError(err)
				err = InsertT1(db, uids[j])
				CheckError(err)

			}(j)
		}
		wg.Wait()
	}

	yu := len(uids) % def
	//模除部分处理
	if yu != 0 {

		for j := stepth * def; j < len(uids); j++ {

			time.Sleep(1 * time.Millisecond)
			fmt.Println("现在初始化userid", uids[j])
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				//todo .. 处理每个用户，从小米获取信息，处理信息并入库
				fmt.Println("hi,Sync is running in yu ")
				err := AssignUserHourData(db, uids[j])
				CheckError(err)
				err = InsertT1(db, uids[j])
				CheckError(err)

			}(j)
		}
		wg.Wait()
	}
}

//需要初始化的人群，开始并行初始化，每次def并发量
func Sync_x(uids []*UserDayData, db *sql.DB, def int) {

	stepth := len(uids) / def
	//fmt.Println("stepth is: ", stepth)

	for i := 0; i < stepth; i++ {

		time.Sleep(1 * time.Millisecond)

		for j := i * def; j < (i+1)*def; j++ {

			wg.Add(1)

			go func(j int) {
				defer wg.Done()
				//todo .. 处理每个用户，从小米获取信息，处理信息并入库
				fmt.Println("hi,Sync is running in batch ")
				err := AssignUserHourData_x(db, uids[j])
				CheckError(err)
				err = InsertT1(db, uids[j])
				CheckError(err)

			}(j)
		}
		wg.Wait()
	}

	yu := len(uids) % def
	//模除部分处理
	if yu != 0 {

		for j := stepth * def; j < len(uids); j++ {

			time.Sleep(1 * time.Millisecond)
			fmt.Println("现在初始化userid", uids[j])
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				//todo .. 处理每个用户，从小米获取信息，处理信息并入库
				fmt.Println("hi,Sync is running in yu ")
				err := AssignUserHourData_x(db, uids[j])
				CheckError(err)
				err = InsertT1(db, uids[j])
				CheckError(err)

			}(j)
		}
		wg.Wait()
	}
}
