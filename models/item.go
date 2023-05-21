package models

type Item struct {
	ID  string `db:"id"`
	Val string `db:"val"`
}
