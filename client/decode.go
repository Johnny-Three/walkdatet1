package client

import (
	"errors"
	"fmt"
	"github.com/bitly/go-simplejson"
	"strings"
	. "wbproject/walkdatet1/structure"
	. "wbproject/walkdatet1/util"
)

func Decode(msg string) error {

	js, err := simplejson.NewJson([]byte(msg))
	if err != nil {
		errback := fmt.Sprintf("decode json error the error msg is %s", err.Error())
		return errors.New(errback)
	}

	var wd WalkDayData
	walkdays := []WalkDayData{}
	userwalkdata := User_walkdays_struct{}

	userid := js.Get("userid").MustInt()
	wd.Timestamp = js.Get("timestamp").MustInt64()
	arr, _ := js.Get("walkdays").Array()

	for index, _ := range arr {

		walkdate := js.Get("walkdays").GetIndex(index).Get("walkdate").MustInt64()
		wd.WalkDate = walkdate

		var err0 error
		walkhour := js.Get("walkdays").GetIndex(index).Get("walkhour").MustString()
		wd.Hourdata, err0 = Slice_Atoi(strings.Split(walkhour, ","))
		if err0 == nil {

			if len(wd.Hourdata) != 24 {
				errback := fmt.Sprintf("uid %d walkdate %d get wrong hourdata %v format", userid, walkdate, wd.Hourdata)
				return errors.New(errback)
			}
		}

		wd.Daydata = js.Get("walkdays").GetIndex(index).Get("walktotal").MustInt()
		wd.Faststepnum = js.Get("walkdays").GetIndex(index).Get("fast").MustInt()
		wd.Effecitvestepnum = js.Get("walkdays").GetIndex(index).Get("effective").MustInt()
		s_recipe := js.Get("walkdays").GetIndex(index).Get("recipe").MustString()
		i_recipe, err1 := Slice_Atoi(strings.Split(s_recipe, ","))
		if err1 == nil {

			if len(i_recipe) != 3 {
				errback := fmt.Sprintf("uid %d walkdate %d get wrong recipe %v format", userid, walkdate, i_recipe)
				return errors.New(errback)
			}
		}
		//no problem .. then assign the chufang related value..
		wd.Chufangid = i_recipe[0]
		wd.Chufangfinish = i_recipe[1]
		wd.Chufangtotal = i_recipe[2]

		//用户此次上传的数据消息存储在MAP中..
		walkdays = append(walkdays, wd)

	}

	userwalkdata.Uid = userid
	userwalkdata.Walkdays = walkdays

	User_walk_data_chan <- userwalkdata

	return nil
}

func init() {

	User_walk_data_chan = make(chan User_walkdays_struct, 16)
}
