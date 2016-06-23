package structure

import (
	"errors"
	//"sort"
	//"fmt"
	"strconv"
	"strings"
	. "wbproject/walkdatet1/util"
)

var User_walk_data_chan chan User_walkdays_struct

type User_walkdays_struct struct {
	Uid      int
	Walkdays []WalkDayData
}

type WalkDayData struct {
	Daydata          int
	Hourdata         []int
	Chufangid        int
	Chufangfinish    int
	Chufangtotal     int
	Faststepnum      int
	Effecitvestepnum int
	WalkDate         int64
	Timestamp        int64
}

type UserDayData struct {
	Userid      int
	Startdate   int64
	Enddate     int64
	MapHourData map[int64]HourData
}

type HourData struct {
	Strhour          []string
	Inthour          []int
	Zmflag           int
	Faststepnum      int
	Effecitvestepnum int
	Walkdate         int64
	Zmstatus         string
	Zmrule           string
}

//***********************************小时数据旧版*******************************************
//clear
func (t *HourData) Clear() {

	t.Strhour = nil
	t.Inthour = nil
	t.Zmflag = 0
	t.Faststepnum = 0
	t.Effecitvestepnum = 0
	t.Walkdate = 0
	t.Zmstatus = ""
	t.Zmrule = ""
}

//计算inthour及剩余有效、快走步数
func (t *HourData) AssignInthour() error {

	if len(t.Inthour) > 0 {

		return errors.New("Inthour已有数据,请勿重复初始化")
	}

	if len(t.Strhour) != 24 {
		return errors.New("天小时数据加载长度不为24，格式错误，请检查")
	}

	var duan int
	fast, effect := 0, 0

	for index, shour := range t.Strhour {

		//hourSteps := 0

		var hourSteps int

		tmp := strings.Split(shour, ",")
		//拿到hour0数据进行解析
		if index == 0 {
			if len(tmp) == 4 {
				//4位数据
				duan = 0
			} else if len(tmp) == 6 {
				//6位数据
				duan = 1
			} else {
				t.Clear()
				return errors.New("小时数据格式错误，既不是4位，也不是6位")
			}
		}

		if duan == 0 {

			tmp0, _ := strconv.Atoi(tmp[0])
			tmp2, _ := strconv.Atoi(tmp[2])
			hourSteps = hourSteps + tmp0 + tmp2
			fast += tmp2
		}

		if duan == 1 {

			tmp0, _ := strconv.Atoi(tmp[0])
			tmp2, _ := strconv.Atoi(tmp[2])
			tmp4, _ := strconv.Atoi(tmp[4])

			hourSteps += tmp0
			fast += tmp2
			effect += tmp4
		}

		t.Inthour = append(t.Inthour, hourSteps)
	}

	t.Faststepnum = fast
	t.Effecitvestepnum = effect

	return nil
}

func (t *HourData) AssignZmflag() error {

	if len(t.Inthour) != 24 {

		return errors.New("Inthour长度不够24，尚未初始化")
	}
	total := 0
	for i := 5; i <= 8; i++ {
		total += t.Inthour[i]
	}
	if total >= 3000 {

		t.Zmflag = 1
		return nil
	}
	total = 0
	for i := 18; i <= 23; i++ {

		total += t.Inthour[i]
	}
	if total >= 4000 {

		t.Zmflag = 1
		return nil
	}
	return nil
}

//***********************************朝三暮四规则*******************************************
type Node struct {
	Hour  []int
	Steps int
}

type PrizeRule struct {
	Dbstring string
	Nodes    []*Node
}

//解析PrizeRule
func (t *PrizeRule) Parse() error {
	//朝三暮四和午间，3段为3个都有，2段为朝三暮四
	//5,6,7,8#3000*1;18,19,20,21,22,23#4000*1;
	if t.Dbstring == "" {
		return nil
	}

	tmps := strings.Split(t.Dbstring, ";")

	if len(tmps) < 2 {

		return errors.New("PrizeRule 格式错误，雷打不动两段值")
	}

	for _, tmp := range tmps {

		a := strings.Split(tmp, "#")
		b := strings.Split(a[0], ",")
		g, err := Slice_Atoi(b)
		if err != nil {

			t.Nodes = nil
			return errors.New("PrizeRule 格式错误,请注意检查")
		}
		c, _ := strconv.Atoi(a[1])
		t.Nodes = append(t.Nodes, &Node{g, c})
	}
	return nil
}

//解析PrizeRule,针对表中数据
func (t *PrizeRule) CalculateOld(wd *HourData) (zrb string, err error) {

	if t.Nodes == nil {
		return "", errors.New("PrizeRule nil")
	}
	var zhao, mu bool
	var zr = "0,"
	for index, node := range t.Nodes {

		var hoursteps int
		//5,6,7,8#3000;18,19,20,21,22,23#4000;
		for _, v := range node.Hour {
			//Attention !! 超过23点不予以考虑暮四成绩
			if v > 23 {
				break
			}
			hoursteps += wd.Inthour[v]
		}
		if hoursteps >= node.Steps {

			if index == 1 {
				zhao = true
			} else if index == 2 {
				mu = true
			}
		}
	}

	if zhao == true {
		zr = "1,"
	}
	if mu == true {
		zr = zr + "1"
	} else {

		zr = zr + "0"
	}

	return zr, nil
}

//解析PrizeRule,针对消息
func (t *PrizeRule) CalculateNew(wd *WalkDayData) (zrb string, err error) {

	if t.Nodes == nil {
		return "", errors.New("PrizeRule nil")
	}
	var zhao, mu bool
	var zr = "0,"
	for index, node := range t.Nodes {

		var hoursteps int
		//5,6,7,8#3000;18,19,20,21,22,23#4000;
		for _, v := range node.Hour {
			//Attention !! 超过23点不予以考虑暮四成绩
			if v > 23 {
				break
			}
			hoursteps += wd.Hourdata[v]
		}
		if hoursteps >= node.Steps {

			if index == 1 {
				zhao = true
			} else if index == 2 {
				mu = true
			}
		}
	}

	if zhao == true {
		zr = "1,"
	}
	if mu == true {
		zr = zr + "1"
	} else {

		zr = zr + "0"
	}

	return zr, nil
}
