package main

import (
	"fmt"
	"os"

	"github.com/go-bond/bond"
)

var fixtureKey = bond.KeyEncode(bond.Key{
	TableID:    0xc0,
	IndexID:    bond.PrimaryIndexID,
	Index:      []byte{},
	IndexOrder: []byte{},
	PrimaryKey: []byte("pre-upgrade-key"),
})

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: preupgrade create|open DATABASE")
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "create":
		err = create(os.Args[2])
	case "open":
		err = open(os.Args[2])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func create(dir string) error {
	db, err := bond.Open(dir, bond.DefaultOptions(bond.MediumPerformance))
	if err != nil {
		return err
	}
	if err := db.Set(fixtureKey, []byte("pre-upgrade-value"), bond.Sync); err != nil {
		_ = db.Close()
		return err
	}
	return db.Close()
}

func open(dir string) error {
	db, err := bond.Open(dir, bond.DefaultOptions(bond.MediumPerformance))
	if err != nil {
		return err
	}
	defer db.Close()

	value, closer, err := db.Get(fixtureKey)
	if err != nil {
		return err
	}
	defer closer.Close()
	if string(value) != "pre-upgrade-value" {
		return fmt.Errorf("unexpected fixture value %q", value)
	}
	return nil
}
