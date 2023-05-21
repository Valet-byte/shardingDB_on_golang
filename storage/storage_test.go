package storage

import (
	"awesomeProject/models"
	"fmt"
	"github.com/jmoiron/sqlx"
	"testing"
)

func initDBConnection() map[uint32]*sqlx.DB {
	db, err := sqlx.Connect("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		fmt.Println("Failed to connect to database:", err)
		panic(err)
	}
	db2, err := sqlx.Connect("postgres", "host=localhost port=5433 user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		fmt.Println("Failed to connect to database:", err)
		panic(err)
	}

	return map[uint32]*sqlx.DB{0: db, 1: db2}
}

func clearBD() {
	st.shardMap[0].Exec("TRUNCATE TABLE item")
	st.shardMap[1].Exec("TRUNCATE TABLE item")
}

var st = NewStorage(initDBConnection())

func TestStorage(t *testing.T) {

	var i1 = models.Item{ID: "", Val: "228"}
	var i2 = models.Item{ID: "123", Val: "229"}
	var i3 = models.Item{ID: "124", Val: "227"}
	item1, err := st.AddItem(i1)
	if err != nil {
		clearBD()
		t.Error("Failed add i1! err: { ", err, " }")
		return
	}

	if item1.Val != i1.Val || item1.ID == i1.ID {
		clearBD()
		t.Error("Failed add i1! err: { not equal Val or empty ID }")
		return
	}

	item2, err := st.AddItem(i2)
	if err != nil {
		clearBD()
		t.Error("Failed add i2! err: { ", err, " }")
		return
	}

	if item2.Val != i2.Val && item2.ID != i2.ID {
		t.Error("Failed add i2! err: { not equal Val or equal ID }")
		clearBD()
		return
	}

	item3, err := st.AddItem(i3)
	if err != nil {
		clearBD()
		t.Error("Failed add i3! err: { ", err, " }")
		return
	}

	if item3.Val != i3.Val || item3.ID != i3.ID {
		clearBD()
		t.Error("Failed add i3! err: { not equal Val or equal ID }")
		return
	}

	t.Log("{ save data test OK}")

	data1, err := st.GetItemById(item1.ID)
	if err != nil {
		clearBD()
		t.Error("Failed get i1! err: { ", err, " }")
		return
	}

	if data1.ID != item1.ID || data1.Val != i1.Val {
		clearBD()
		t.Error("Failed get i1! err: { the data do not match }")
		return
	}

	data2, err := st.GetItemById(i2.ID)
	if err != nil {
		clearBD()
		t.Error("Failed get i2! err: { ", err, " }")
		return
	}

	if data2.ID != i2.ID || data2.Val != i2.Val {
		clearBD()
		t.Error("Failed get i2! err: { the data do not match }")
		return
	}

	data3, err := st.GetItemById(i3.ID)
	if err != nil {
		clearBD()
		t.Error("Failed get i3! err: { ", err, " }")
		return
	}

	if data3.ID != i3.ID || data3.Val != i3.Val {
		clearBD()
		t.Error("Failed get i3! err: { the data do not match }")
		return
	}
	clearBD()
	t.Log("{ get data test OK}")
}
