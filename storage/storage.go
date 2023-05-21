package storage

import (
	"awesomeProject/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"hash/fnv"
)

const bucketQuantity = 2 // колличество шардов

const (
	selectItemByIdQuery = "SELECT * FROM item WHERE id = $1"
	insertItemQuery     = "INSERT INTO item values ($1 , $2)"
)

type shardMap map[uint32]*sqlx.DB

type Storage struct {
	shardMap shardMap
}

func NewStorage(dbs map[uint32]*sqlx.DB) *Storage {
	return &Storage{
		shardMap: dbs,
	}
}

func generateUUID() string {
	u := uuid.New()
	return u.String()
}

func getShardNumByItemId(id string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(id))
	if err != nil {
		return 0
	}
	return h.Sum32() % bucketQuantity
}

func (s *Storage) GetItemById(id string) (*models.Item, error) {
	var item models.Item

	err := s.shardMap[getShardNumByItemId(id)].Get(&item, selectItemByIdQuery, id)
	if err != nil {
		return nil, err
	}

	return &item, nil
}

func (s *Storage) AddItem(item models.Item) (*models.Item, error) {
	if item.ID == "" {
		item.ID = generateUUID()
	}

	_, err := s.shardMap[getShardNumByItemId(item.ID)].Exec(insertItemQuery, item.ID, item.Val)
	if err != nil {
		return nil, err
	}

	return &item, nil
}
