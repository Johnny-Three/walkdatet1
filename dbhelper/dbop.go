package dbhelper

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	. "wbproject/walkdatet1/logs"
	. "wbproject/walkdatet1/structure"
)

var err error

func StatTrigger(user *UserDayData, db *sql.DB) error {

	sqlStr := fmt.Sprintf("INSERT INTO `wanbu_data_uploadqueue_user` (`userid`, `timestamp`, `walkdate`) VALUES (%d,UNIX_TIMESTAMP(),UNIX_TIMESTAMP(20160601))", user.Userid)

	_, err := db.Exec(sqlStr)

	fmt.Println("StatTrigger:", sqlStr)
	Logger.Info("StatTrigger:", sqlStr)

	if err != nil {
		return err
	}

	return nil
}

func SelectInitUsers(db *sql.DB) ([]*UserDayData, error) {

	users := []*UserDayData{}

	//半年内上传过数据的人
	//qs := "select userid,unix_timestamp(from_unixtime(lastuploadtime,'%Y-%m-%d')) from wanbu_data_userdevice where lastuploadtime > unix_timestamp(date_sub(curdate(),interval 6 month)) limit 10"
	qs := "SELECT de.userid,unix_timestamp(from_unixtime(unix_timestamp(),'%Y-%m-%d')) FROM wanbu_data_userdevice de,wanbu_stat_user sa WHERE de.lastuploadtime>=UNIX_TIMESTAMP(20160601) AND de.userid =sa.userid AND sa.stepdaysa>=1"

	rows, err := db.Query(qs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {

		user := UserDayData{}
		user.MapHourData = make(map[int64]HourData)

		err := rows.Scan(&user.Userid, &user.Enddate)
		if err != nil {
			return nil, err
		}
		users = append(users, &user)
	}
	return users, nil

}

//========================初始化=========================
func SelectAllUsers(db *sql.DB) ([]*UserDayData, error) {

	users := []*UserDayData{}

	//半年内上传过数据的人
	//qs := "select userid,unix_timestamp(from_unixtime(lastuploadtime,'%Y-%m-%d')) from wanbu_data_userdevice where lastuploadtime > unix_timestamp(date_sub(curdate(),interval 6 month)) limit 10"
	qs := "select userid,unix_timestamp(from_unixtime(unix_timestamp(),'%Y-%m-%d')) from wanbu_data_userdevice where lastuploadtime > unix_timestamp(date_sub(curdate(),interval 6 month)) "

	rows, err := db.Query(qs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {

		user := UserDayData{}
		user.MapHourData = make(map[int64]HourData)

		err := rows.Scan(&user.Userid, &user.Enddate)
		if err != nil {
			return nil, err
		}
		users = append(users, &user)
	}
	return users, nil
}

func AssingOneUserBeginDate_x(db *sql.DB, user *UserDayData) error {

	//找到开始时间,如果min(walkdate)为空，则为异常数据
	qs := "select IFNULL(min(walkdate),-1) from wanbu_data_walkday where userid = ?"

	rows, err0 := db.Query(qs, user.Userid)
	if err0 != nil {
		return err0
	}
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&user.Startdate)
		if err != nil {
			return err
		}
	}
	if user.Startdate == -1 {

		errback := fmt.Sprintf("userid:%d,lastuploadtime:%d,根据条件select IFNULL(min(walkdate),-1) from wanbu_data_walkday where userid =?", user.Userid, user.Enddate)
		return errors.New(errback)
	}
	return nil
}

func AssingOneUserBeginDate(db *sql.DB, user *UserDayData) error {

	//找到开始时间,如果min(walkdate)为空，则为异常数据
	qs := "select IFNULL(min(walkdate),-1) from wanbu_data_walkday where walkdate >= unix_timestamp(date_format(date_sub(curdate(),interval 6 month),'%Y-%m-%d')) and userid = ?"

	rows, err0 := db.Query(qs, user.Userid)
	if err0 != nil {
		return err0
	}
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&user.Startdate)
		if err != nil {
			return err
		}
	}
	if user.Startdate == -1 {

		errback := fmt.Sprintf("userid:%d,lastuploadtime:%d,根据条件walkdate >= unix_timestamp(date_format(date_sub(curdate(),interval 6 month)查找wanbu_data_walkday数据异常·", user.Userid, user.Enddate)
		return errors.New(errback)
	}
	return nil
}

//针对某用户某天的小时数据进行数据初始化
func AssignOneUserHourData(db *sql.DB, userid int, walkdate int64, zmrule string, hour *HourData) (bool, error) {

	qs := "select hour2,hour3,hour4,hour5,hour6,hour7,hour8,hour9,hour10,hour11,hour12,hour13,hour14,hour15,hour16,hour17,hour18,hour19,hour20,hour21,hour22,hour23,hour24,hour25 from wanbu_data_walkhour where userid=? and walkdate = ? "

	var hour0, hour1, hour2, hour3, hour4, hour5, hour6, hour7, hour8, hour9, hour10, hour11, hour12, hour13, hour14, hour15, hour16, hour17, hour18, hour19, hour20, hour21, hour22, hour23 string

	exists := false
	rows, err0 := db.Query(qs, userid, walkdate)
	if err0 != nil {
		return false, err0
	}
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&hour0, &hour1, &hour2, &hour3, &hour4, &hour5, &hour6, &hour7, &hour8, &hour9, &hour10, &hour11, &hour12, &hour13, &hour14, &hour15, &hour16, &hour17, &hour18, &hour19, &hour20, &hour21, &hour22, &hour23)
		if err != nil {
			return false, err
		}

		exists = true
	}

	if exists == false {

		return false, nil
	}

	hour.Strhour = append(hour.Strhour, hour0)
	hour.Strhour = append(hour.Strhour, hour1)
	hour.Strhour = append(hour.Strhour, hour2)
	hour.Strhour = append(hour.Strhour, hour3)
	hour.Strhour = append(hour.Strhour, hour4)
	hour.Strhour = append(hour.Strhour, hour5)
	hour.Strhour = append(hour.Strhour, hour6)
	hour.Strhour = append(hour.Strhour, hour7)
	hour.Strhour = append(hour.Strhour, hour8)
	hour.Strhour = append(hour.Strhour, hour9)
	hour.Strhour = append(hour.Strhour, hour10)
	hour.Strhour = append(hour.Strhour, hour11)
	hour.Strhour = append(hour.Strhour, hour12)
	hour.Strhour = append(hour.Strhour, hour13)
	hour.Strhour = append(hour.Strhour, hour14)
	hour.Strhour = append(hour.Strhour, hour15)
	hour.Strhour = append(hour.Strhour, hour16)
	hour.Strhour = append(hour.Strhour, hour17)
	hour.Strhour = append(hour.Strhour, hour18)
	hour.Strhour = append(hour.Strhour, hour19)
	hour.Strhour = append(hour.Strhour, hour20)
	hour.Strhour = append(hour.Strhour, hour21)
	hour.Strhour = append(hour.Strhour, hour22)
	hour.Strhour = append(hour.Strhour, hour23)

	//计算快走及剩余有效步数
	fmt.Println("userid:", userid, "walkdate:", walkdate, "hour.Strhour:", hour.Strhour)
	err = hour.AssignInthour()
	if err != nil {
		return false, err
	}
	//计算zmflag
	err = hour.AssignZmflag()
	if err != nil {
		return false, err
	}

	//计算zmstatus
	zm := PrizeRule{}
	zm.Dbstring = zmrule
	err = zm.Parse()
	if err != nil {
		return false, err
	}
	zs, err1 := zm.CalculateOld(hour)
	if err1 != nil {
		return false, err1
	}
	hour.Zmstatus = zs
	hour.Zmrule = zmrule
	return true, nil
}

//针对某用户某天的小时数据进行数据初始化
func AssignOneUserHourData1(db *sql.DB, userid int, walkdate int64, hour *HourData) (bool, error) {

	qs := "select hour2,hour3,hour4,hour5,hour6,hour7,hour8,hour9,hour10,hour11,hour12,hour13,hour14,hour15,hour16,hour17,hour18,hour19,hour20,hour21,hour22,hour23,hour24,hour25 from wanbu_data_walkhour where userid=? and walkdate = ? "

	var hour0, hour1, hour2, hour3, hour4, hour5, hour6, hour7, hour8, hour9, hour10, hour11, hour12, hour13, hour14, hour15, hour16, hour17, hour18, hour19, hour20, hour21, hour22, hour23 string

	exists := false
	rows, err0 := db.Query(qs, userid, walkdate)
	if err0 != nil {
		return false, err0
	}
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&hour0, &hour1, &hour2, &hour3, &hour4, &hour5, &hour6, &hour7, &hour8, &hour9, &hour10, &hour11, &hour12, &hour13, &hour14, &hour15, &hour16, &hour17, &hour18, &hour19, &hour20, &hour21, &hour22, &hour23)
		if err != nil {
			return false, err
		}

		exists = true
	}

	if exists == false {

		return false, nil
	}

	hour.Strhour = append(hour.Strhour, hour0)
	hour.Strhour = append(hour.Strhour, hour1)
	hour.Strhour = append(hour.Strhour, hour2)
	hour.Strhour = append(hour.Strhour, hour3)
	hour.Strhour = append(hour.Strhour, hour4)
	hour.Strhour = append(hour.Strhour, hour5)
	hour.Strhour = append(hour.Strhour, hour6)
	hour.Strhour = append(hour.Strhour, hour7)
	hour.Strhour = append(hour.Strhour, hour8)
	hour.Strhour = append(hour.Strhour, hour9)
	hour.Strhour = append(hour.Strhour, hour10)
	hour.Strhour = append(hour.Strhour, hour11)
	hour.Strhour = append(hour.Strhour, hour12)
	hour.Strhour = append(hour.Strhour, hour13)
	hour.Strhour = append(hour.Strhour, hour14)
	hour.Strhour = append(hour.Strhour, hour15)
	hour.Strhour = append(hour.Strhour, hour16)
	hour.Strhour = append(hour.Strhour, hour17)
	hour.Strhour = append(hour.Strhour, hour18)
	hour.Strhour = append(hour.Strhour, hour19)
	hour.Strhour = append(hour.Strhour, hour20)
	hour.Strhour = append(hour.Strhour, hour21)
	hour.Strhour = append(hour.Strhour, hour22)
	hour.Strhour = append(hour.Strhour, hour23)

	//计算快走及剩余有效步数
	err = hour.AssignInthour()
	if err != nil {
		return false, err
	}
	//计算zmflag
	err = hour.AssignZmflag()
	if err != nil {
		return false, err
	}

	return true, nil
}

//查询zmrule，从member_profile表中
func GetZmRule(db *sql.DB, userid int) (string, error) {

	qs := "select addtionrule from wanbu_member_profile where userid = ?"
	rows, err := db.Query(qs, userid)
	if err != nil {
		return "", err
	}
	var zmrule string
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&zmrule)
		if err != nil {
			return "", err
		}
	}
	return zmrule, nil
}

//针对某用户的小时数据进行初始化，从开始时间到结束时间
func AssignUserHourData(db *sql.DB, user *UserDayData) error {

	zmrule, err := GetZmRule(db, user.Userid)
	if err != nil {
		return err
	}
	//todo .. 改一改 ..
	err0 := AssingOneUserBeginDate(db, user)
	if err0 != nil {
		return err0
	}

	//fmt.Println("after assgin begindate", user)
	for wd := user.Startdate; wd <= user.Enddate; wd += 86400 {

		hd := HourData{}
		//fmt.Println("userid:", user.Userid, "walkdate:", wd, "......")
		b, err := AssignOneUserHourData(db, user.Userid, wd, zmrule, &hd)
		if err != nil {
			errback := fmt.Sprintf("userid:%d,walkdate:%d,error:%s", user.Userid, wd, err.Error())
			return errors.New(errback)
		}
		if b == true {
			hd.Walkdate = wd
			user.MapHourData[wd] = hd
		}
	}
	fmt.Printf("UserDayData 用户ID[%d],开始时间[%d],结束时间[%d],初始化数据量[%d]\n", user.Userid, user.Startdate, user.Enddate, len(user.MapHourData))
	return nil
}

//针对某用户的小时数据进行初始化，从开始时间到结束时间
func AssignUserHourData_x(db *sql.DB, user *UserDayData) error {

	err0 := AssingOneUserBeginDate_x(db, user)
	if err0 != nil {
		return err0
	}

	//fmt.Println("after assgin begindate", user)
	for wd := user.Startdate; wd <= user.Enddate; wd += 86400 {

		hd := HourData{}
		//fmt.Println("userid:", user.Userid, "walkdate:", wd, "......")
		b, err := AssignOneUserHourData1(db, user.Userid, wd, &hd)
		if err != nil {

			errback := fmt.Sprintf("userid:%d,walkdate:%d,error:%s", user.Userid, wd, err.Error())
			return errors.New(errback)
		}
		if b == true {
			hd.Walkdate = wd
			user.MapHourData[wd] = hd
		}
	}
	fmt.Printf("UserDayData 用户ID[%d],开始时间[%d],结束时间[%d],初始化数据量[%d]\n", user.Userid, user.Startdate, user.Enddate, len(user.MapHourData))
	return nil
}

func GetZmRuleFromT1(db *sql.DB, userid int, walkdate int64) (string, string, error) {

	qs := "select trim(zmrule),trim(zmstatus) from wanbu_data_walkday_t1 where userid = ? and walkdate = ?"
	rows, err := db.Query(qs, userid, walkdate)
	if err != nil {
		return "", "", err
	}
	var zmrule, zmstatus string
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&zmrule, &zmstatus)
		if err != nil {
			return "", "", err
		}
	}
	return zmrule, zmstatus, nil
}

//NSQ消息中fastnum\effectivenum为-1，需要从DB中计算
func AssignUserHourDataNsq1(db *sql.DB, user *UserDayData, uws *User_walkdays_struct) (int, error) {

	user.Userid = uws.Uid

	//没有ZMRULE，从DB中拿到ZMRULE，ZMSTATUS，更新DB
	zmrule, zmstatus, err := GetZmRuleFromT1(db, user.Userid, uws.Walkdays[len(uws.Walkdays)-1].WalkDate)
	if err != nil {
		fmt.Println("error happens in AssignUserHourDataNsq1")
		return 0, err
	}

	//出现这种情况，补zmrule，zmstatus不能动
	if len(zmrule) == 0 && len(zmstatus) > 0 {

		zmrule, err := GetZmRule(db, user.Userid)
		if err != nil {
			return 0, err
		}

		for _, v := range uws.Walkdays {

			hd := HourData{}
			hd.Zmrule = zmrule
			user.MapHourData[v.WalkDate] = hd
		}
		//需更新ZMRULE，但不更新ZMSTATUS字段
		return 3, nil

	}

	//如果ZMRULE有值，len大于1，那么更新表的时候不需要更新zmrule及zmstatus字段
	//如果ZMSTATUS有值，len大于1，那么更新表的时候不需要更新zmrule及zmstatus字段
	if len(zmrule) > 0 || len(zmstatus) > 0 {

		for _, v := range uws.Walkdays {

			hd := HourData{}

			b, err := AssignOneUserHourData1(db, user.Userid, v.WalkDate, &hd)
			if err != nil {
				errback := fmt.Sprintf("userid:%d,walkdate:%d,error:%s", user.Userid, v.WalkDate, err.Error())
				return 0, errors.New(errback)
			}
			if b == true {
				user.MapHourData[v.WalkDate] = hd
			}
		}
		//无需更新ZMRULE,ZMSTATUS字段
		return 2, nil
	}

	//如果ZMRULE没有值并且ZMSTATUS没有值，那么更新表的时候需要从别处拿到zmrule及zmstatus字段值
	//如果ZMRULE有值并且ZMSTATUS没有值，那么更新表的时候需要从别处拿到zmrule及zmstatus字段值
	if (len(zmrule) == 0 && len(zmstatus) == 0) || (len(zmrule) > 0 && len(zmstatus) == 0) {

		zmrule, err := GetZmRule(db, user.Userid)
		if err != nil {
			return 0, err
		}

		for _, v := range uws.Walkdays {

			hd := HourData{}

			b, err := AssignOneUserHourData(db, user.Userid, v.WalkDate, zmrule, &hd)
			if err != nil {
				errback := fmt.Sprintf("userid:%d,walkdate:%d,error:%s", user.Userid, v.WalkDate, err.Error())
				return 0, errors.New(errback)
			}
			if b == true {
				user.MapHourData[v.WalkDate] = hd
			}
		}
		//需更新ZMRULE,ZMSTATUS字段
		return 1, nil
	}

	return 0, errors.New("未知错误")
}

//NSQ消息中自带fastnum\effectivenum值，直接赋值即可
func AssignUserHourDataNsq2(db *sql.DB, user *UserDayData, uws *User_walkdays_struct) (int, error) {

	user.Userid = uws.Uid

	//没有ZMRULE，从DB中拿到ZMRULE，ZMSTATUS，更新DB (从天数据中拿一条数据即可)
	zmrule, zmstatus, err := GetZmRuleFromT1(db, user.Userid, uws.Walkdays[len(uws.Walkdays)-1].WalkDate)

	//fmt.Println("userid is", user.Userid, "walkdate is:", uws.Walkdays[0].WalkDate, "zmrule is:", zmrule)
	if err != nil {
		fmt.Println("error happens in AssignUserHourDataNsq2")
		return 0, err
	}

	//出现这种情况，补zmrule，zmstatus不能动
	if len(zmrule) == 0 && len(zmstatus) > 0 {

		zmrule, err := GetZmRule(db, user.Userid)
		if err != nil {
			return 0, err
		}

		for _, v := range uws.Walkdays {

			hd := HourData{}
			hd.Zmrule = zmrule
			//小时数据转移到另外一个数据结构中
			hd.Inthour = v.Hourdata
			hd.Effecitvestepnum = v.Effecitvestepnum
			hd.Faststepnum = v.Faststepnum
			//计算zmflag
			err = hd.AssignZmflag()
			if err != nil {
				return 0, err
			}
			user.MapHourData[v.WalkDate] = hd
		}
		//需更新ZMRULE，但不更新ZMSTATUS字段
		return 3, nil

	}

	//如果ZMRULE及ZMSTATUS有值，len大于1，那么更新表的时候不需要更新zmrule及zmstatus字段
	if len(zmrule) > 0 && len(zmstatus) > 0 {

		for _, v := range uws.Walkdays {

			hd := HourData{}
			//小时数据转移到另外一个数据结构中
			hd.Inthour = v.Hourdata
			hd.Effecitvestepnum = v.Effecitvestepnum
			hd.Faststepnum = v.Faststepnum
			//计算zmflag
			err = hd.AssignZmflag()
			if err != nil {
				return 0, err
			}
			user.MapHourData[v.WalkDate] = hd
		}
		//无需更新ZMRULE,ZMSTATUS字段
		return 2, nil
	}

	//如果ZMRULE没有值并且ZMSTATUS没有值，那么更新表的时候需要从别处拿到zmrule及zmstatus字段值
	//如果ZMRULE有值并且ZMSTATUS没有值，那么更新表的时候需要从别处拿到zmrule及zmstatus字段值
	if (len(zmrule) == 0 && len(zmstatus) == 0) || (len(zmrule) > 0 && len(zmstatus) == 0) {

		zmrule, err := GetZmRule(db, user.Userid)
		if err != nil {
			return 0, err
		}

		for _, v := range uws.Walkdays {

			hd := HourData{}
			//小时数据转移到另外一个数据结构中
			hd.Inthour = v.Hourdata
			hd.Effecitvestepnum = v.Effecitvestepnum
			hd.Faststepnum = v.Faststepnum
			//计算zmflag
			err = hd.AssignZmflag()
			if err != nil {
				return 0, err
			}
			//计算zmstatus
			zm := PrizeRule{}
			zm.Dbstring = zmrule
			err = zm.Parse()
			if err != nil {
				return 0, err
			}
			zs, err1 := zm.CalculateOld(&hd)
			if err1 != nil {
				return 0, err1
			}
			hd.Zmrule = zmrule
			hd.Zmstatus = zs
			user.MapHourData[v.WalkDate] = hd
		}
		//需更新ZMRULE,ZMSTATUS字段
		return 1, nil
	}

	return 0, errors.New("未知错误")
}

//需要更新ZMRULE
func InsertT1N1(db *sql.DB, user *UserDayData) error {

	if len(user.MapHourData) == 0 {

		return nil
	}

	for key, value := range user.MapHourData {

		sqlStr := fmt.Sprintf("Update wanbu_data_walkday_t1 set zmflag = %d,faststepnum = %d,remaineffectiveSteps=%d,zmrule = '%s',zmstatus='%s' where userid = %d and walkdate = %d", value.Zmflag, value.Faststepnum, value.Effecitvestepnum, value.Zmrule, value.Zmstatus, user.Userid, key)

		_, err := db.Exec(sqlStr)

		fmt.Println("InsertT1N1:", sqlStr)
		Logger.Info("InsertT1N1:", sqlStr)

		if err != nil {
			return err
		}
	}
	return nil
}

//无需更新ZMRULE
func InsertT1N2(db *sql.DB, user *UserDayData) error {

	if len(user.MapHourData) == 0 {

		return nil
	}

	for key, value := range user.MapHourData {

		sqlStr := fmt.Sprintf("Update wanbu_data_walkday_t1 set zmflag = %d,faststepnum = %d,remaineffectiveSteps=%d where userid = %d and walkdate = %d", value.Zmflag, value.Faststepnum, value.Effecitvestepnum, user.Userid, key)

		_, err := db.Exec(sqlStr)

		fmt.Println("InsertT1N2:", sqlStr)
		Logger.Info("InsertT1N2:", sqlStr)

		if err != nil {
			return err
		}
	}
	return nil
}

//需要更新ZMRULE,但不更新ZMSTATUS
func InsertT1N3(db *sql.DB, user *UserDayData) error {

	if len(user.MapHourData) == 0 {

		return nil
	}

	for key, value := range user.MapHourData {

		sqlStr := fmt.Sprintf("Update wanbu_data_walkday_t1 set zmflag = %d,faststepnum = %d,remaineffectiveSteps=%d,zmrule = '%s' where userid = %d and walkdate = %d", value.Zmflag, value.Faststepnum, value.Effecitvestepnum, value.Zmrule, user.Userid, key)

		_, err := db.Exec(sqlStr)

		fmt.Println("InsertT1N3:", sqlStr)
		Logger.Info("InsertT1N3:", sqlStr)

		if err != nil {
			return err
		}
	}
	return nil
}

func InsertT1(db *sql.DB, user *UserDayData) error {

	if len(user.MapHourData) == 0 {

		return nil
	}
	var id int
	for key, value := range user.MapHourData {

		qs := fmt.Sprintf("select dataid from wanbu_data_walkday where userid=%d and walkdate =%d", user.Userid, value.Walkdate)
		err = db.QueryRow(qs).Scan(&id)
		if err != nil {

			errback := fmt.Sprintf("InsertT1 err:%s,can not find data in wanbu_data_walkday,userid:%d,walkdate:%d", err, user.Userid, value.Walkdate)
			Logger.Critical(errback)
			continue
		}

		sqlStr := `
	   INSERT INTO wanbu_data_walkday_t1 (dataid,userid, walkdate, zmflag, faststepnum, remaineffectiveSteps, zmrule, zmstatus) values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE dataid=VALUES(dataid),walkdate = VALUES(walkdate),zmflag = VALUES(zmflag),faststepnum = VALUES(faststepnum),remaineffectiveSteps = VALUES(remaineffectiveSteps),zmrule = VALUES(zmrule),zmstatus = VALUES(zmstatus)`

		_, err := db.Exec(sqlStr, id, user.Userid, key, value.Zmflag, value.Faststepnum, value.Effecitvestepnum, value.Zmrule, value.Zmstatus)

		if err != nil {
			return err
		}
	}
	fmt.Printf("处理完毕%d,开始时间%d,结束时间%d\n", user.Userid, user.Startdate, user.Enddate)
	Logger.Infof("处理完毕%d,开始时间%d,结束时间%d\n", user.Userid, user.Startdate, user.Enddate)

	return nil
}
